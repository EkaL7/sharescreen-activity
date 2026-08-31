import 'dotenv/config';
import express from 'express';
import { createServer } from 'http';
import { createServer as createHttpsServer } from 'https';
import { readFileSync, existsSync } from 'fs';
import { Server } from 'socket.io';
import { WebSocketServer } from 'ws';
import { fileURLToPath } from 'url';
import path from 'path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();

const tlsCert = process.env.TLS_CERT || '';
const tlsKey = process.env.TLS_KEY || '';
let httpServer;
if (tlsCert && tlsKey && existsSync(tlsCert) && existsSync(tlsKey)) {
  httpServer = createHttpsServer({ cert: readFileSync(tlsCert), key: readFileSync(tlsKey) }, app);
  console.log('[tls] HTTPS ativo');
} else {
  httpServer = createServer(app);
  console.log('[tls] HTTP (sem certificado)');
}

const io = new Server(httpServer, { cors: { origin: '*' }, maxHttpBufferSize: 4e6 });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'dist')));

app.post('/api/token', async (req, res) => {
  try {
    const response = await fetch('https://discord.com/api/oauth2/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: process.env.DISCORD_CLIENT_ID,
        client_secret: process.env.DISCORD_CLIENT_SECRET,
        grant_type: 'authorization_code',
        code: req.body.code,
      }),
    });
    const data = await response.json();
    if (!data.access_token) return res.status(400).json({ error: 'Token exchange failed' });
    res.json({ access_token: data.access_token });
  } catch { res.status(500).json({ error: 'Internal error' }); }
});

app.get('/stream', (req, res) => res.sendFile(path.join(__dirname, 'public', 'share.html')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'dist', 'index.html')));

// ────────────────────────────────────────────── Protocolo binário
// [1B slot][1B tipo][8B timestamp][8B relógio de envio][payload]
const TIPO_KEYFRAME = 1;
const TIPO_DELTA = 2;
const TIPO_AUDIO = 3;
const TIPO_THUMB = 4;

// Backpressure por viewer: um viewer lento não pode acumular buffer infinito.
// Deltas são descartáveis SÓ se marcarmos o viewer como precisando de keyframe
// — um delta perdido corrompe o vídeo até o próximo keyframe chegar.
const VIEWER_BUFFER_SOFT = 1_500_000; // acima disso, dropa deltas
const VIEWER_BUFFER_HARD = 4_000_000; // acima disso, dropa até keyframes
const THUMB_MAX_BYTES = 120_000;

// ────────────────────────────────────────────── Rooms
const rooms = new Map();

function getRoom(id) {
  if (!rooms.has(id)) rooms.set(id, {
    participants: new Map(), // socketId → {userId, username, avatarUrl}
    streamers: new Map(),    // streamerId → {ws, username, fonte, slot, decoderConfig, audioConfig, thumbnail, lastKeyReq}
    watchers: new Map(),     // viewerSocketId → {streamerId, needsKey}
    nextSlot: 1,
  });
  return rooms.get(id);
}

function countViewers(room, streamerId) {
  let n = 0;
  room.watchers.forEach(w => { if (w.streamerId === streamerId) n++; });
  return n;
}

function broadcastViewerCount(room, streamerId) {
  const s = room.streamers.get(streamerId);
  if (s?.ws?.readyState === 1) {
    s.ws.send(JSON.stringify({ type: 'state', viewers: countViewers(room, streamerId) }));
  }
}

function requestKeyframe(room, streamerId) {
  const s = room.streamers.get(streamerId);
  if (!s || s.ws.readyState !== 1) return;
  const now = Date.now();
  if (now - (s.lastKeyReq || 0) < 1000) return; // no máx. 1 pedido/s
  s.lastKeyReq = now;
  s.ws.send(JSON.stringify({ type: 'need-keyframe' }));
}

/** bufferedAmount do transporte real do viewer, quando acessível. */
function viewerBuffered(sock) {
  const raw = sock?.conn?.transport?.socket;
  return (raw && typeof raw.bufferedAmount === 'number') ? raw.bufferedAmount : 0;
}

// Índice rápido: streamerId → Set<viewerSocketId>
// Evita iterar TODOS os watchers da sala pra cada frame.
const viewersByStreamer = new Map();

function addViewer(streamerId, viewerId) {
  let s = viewersByStreamer.get(streamerId);
  if (!s) { s = new Set(); viewersByStreamer.set(streamerId, s); }
  s.add(viewerId);
}

function removeViewer(streamerId, viewerId) {
  const s = viewersByStreamer.get(streamerId);
  if (s) { s.delete(viewerId); if (s.size === 0) viewersByStreamer.delete(streamerId); }
}

function relayFrame(room, streamerId, buf, tipo) {
  const viewers = viewersByStreamer.get(streamerId);
  if (!viewers || viewers.size === 0) return;

  const isDelta = tipo === TIPO_DELTA;
  const isKey = tipo === TIPO_KEYFRAME;

  for (const viewerId of viewers) {
    const w = room.watchers.get(viewerId);
    if (!w) continue;
    const sock = io.sockets.sockets.get(viewerId);
    if (!sock) continue;

    if (tipo === TIPO_AUDIO) { sock.emit('stream-data', buf); continue; }

    const buffered = viewerBuffered(sock);

    if (isDelta) {
      if (w.needsKey) continue;
      if (buffered > VIEWER_BUFFER_SOFT) {
        w.needsKey = true;
        requestKeyframe(room, streamerId);
        continue;
      }
      sock.emit('stream-data', buf);
      continue;
    }

    if (isKey) {
      if (buffered > VIEWER_BUFFER_HARD) {
        w.needsKey = true;
        requestKeyframe(room, streamerId);
        continue;
      }
      w.needsKey = false;
      sock.emit('stream-data', buf);
    }
  }
}

// ──────────────────────────────────── WebSocket relay (streamers em /ws)
const wss = new WebSocketServer({ noServer: true });

httpServer.on('upgrade', (request, socket, head) => {
  let url;
  try { url = new URL(request.url, `http://${request.headers.host}`); }
  catch { socket.destroy(); return; }
  if (url.pathname === '/ws') {
    wss.handleUpgrade(request, socket, head, ws => wss.emit('connection', ws, request));
  }
  // Upgrades de /socket.io/ são tratados pelo próprio Socket.io.
});

wss.on('connection', (ws, request) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const roomId = url.searchParams.get('room');
  const username = (url.searchParams.get('user') || 'Streamer').slice(0, 64);
  const fonte = url.searchParams.get('fonte') === 'camera' ? 'camera' : 'tela';

  if (!roomId) { ws.close(4000, 'Missing room'); return; }

  const room = getRoom(roomId);
  const streamerId = `ws-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const slot = room.nextSlot;
  room.nextSlot = room.nextSlot >= 250 ? 1 : room.nextSlot + 1;

  room.streamers.set(streamerId, {
    ws, username, fonte, slot,
    decoderConfig: null, audioConfig: null, thumbnail: null, lastKeyReq: 0,
  });

  ws.send(JSON.stringify({ type: 'slot', slot }));
  ws.send(JSON.stringify({ type: 'state', viewers: 0 }));

  io.to(roomId).emit('streamer-added', { socketId: streamerId, username, fonte });
  console.log(`[ws] ${username} (${fonte}) conectou — sala ${roomId.slice(0, 12)}`);

  ws.on('message', (data, isBinary) => {
    if (!isBinary) {
      try {
        const msg = JSON.parse(data.toString());
        const s = room.streamers.get(streamerId);
        if (!s) return;
        if (msg.type === 'config') {
          console.log(`[ws] ${username} enviou decoder config: ${msg.config?.codec}`);
          s.decoderConfig = msg.config;
          room.watchers.forEach((w, viewerId) => {
            if (w.streamerId === streamerId) {
              io.to(viewerId).emit('decoder-config', { streamerId, config: msg.config, slot });
            }
          });
        } else if (msg.type === 'audio-config') {
          s.audioConfig = msg.config;
          room.watchers.forEach((w, viewerId) => {
            if (w.streamerId === streamerId) {
              io.to(viewerId).emit('audio-config', { streamerId, config: msg.config });
            }
          });
        } else if (msg.type === 'start') {
          console.log(`[ws] ${username} começou a transmitir`);
        } else if (msg.type === 'stop') {
          console.log(`[ws] ${username} parou`);
        }
      } catch {}
      return;
    }

    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (buf.length < 18) return;
    const tipo = buf[1];

    if (tipo === TIPO_THUMB) {
      const payload = Buffer.from(buf.subarray(18));
      if (payload.length > 0 && payload.length <= THUMB_MAX_BYTES) {
        const s = room.streamers.get(streamerId);
        if (s) s.thumbnail = payload;
        // Miniatura vai para a sala toda (cards), não só para quem assiste.
        io.to(roomId).volatile.emit('stream-thumbnail', { streamerId, data: payload });
      }
      return;
    }

    relayFrame(room, streamerId, buf, tipo);
  });

  ws.on('close', () => {
    room.streamers.delete(streamerId);
    viewersByStreamer.delete(streamerId);
    room.watchers.forEach((w, vid) => { if (w.streamerId === streamerId) room.watchers.delete(vid); });
    io.to(roomId).emit('streamer-removed', { socketId: streamerId });
    console.log(`[ws] ${username} desconectou`);
    if (room.participants.size === 0 && room.streamers.size === 0) rooms.delete(roomId);
  });

  ws.on('error', () => {});
});

// ──────────────────────────────── Socket.io (viewers na Activity)
io.on('connection', (socket) => {
  let roomId = null, userData = null;

  socket.on('debug', ({ msg }) => console.log(`[dbg ${socket.id.slice(0, 6)}] ${msg}`));

  socket.on('join-room', (data) => {
    roomId = data.roomId;
    userData = {
      socketId: socket.id,
      userId: data.userId,
      username: String(data.username || 'Usuário').slice(0, 64),
      avatarUrl: data.avatarUrl || '',
    };
    socket.join(roomId);
    const room = getRoom(roomId);
    room.participants.set(socket.id, userData);

    const parts = [], strs = [];
    room.participants.forEach((p, sid) => { if (sid !== socket.id) parts.push(p); });
    room.streamers.forEach((s, sid) => strs.push({
      socketId: sid, username: s.username, fonte: s.fonte, thumbnail: s.thumbnail,
    }));
    socket.emit('room-state', { participants: parts, streamers: strs });
    socket.to(roomId).emit('user-joined', userData);
    console.log(`[room] ${userData.username} entrou (${room.participants.size} na sala)`);
  });

  socket.on('watch-stream', ({ streamerId }) => {
    if (!roomId) return;
    const room = getRoom(roomId);
    console.log(`[watch] ${socket.id.slice(0, 6)} quer assistir ${streamerId?.slice(0, 12) ?? 'null'}`);

    const prev = room.watchers.get(socket.id);
    if (prev) {
      removeViewer(prev.streamerId, socket.id);
      room.watchers.delete(socket.id);
      broadcastViewerCount(room, prev.streamerId);
    }

    if (!streamerId || !room.streamers.has(streamerId)) {
      console.log(`[watch] streamer não encontrado. streamers: [${[...room.streamers.keys()].join(', ')}]`);
      return;
    }

    room.watchers.set(socket.id, { streamerId, needsKey: true });
    addViewer(streamerId, socket.id);
    broadcastViewerCount(room, streamerId);

    const s = room.streamers.get(streamerId);
    console.log(`[watch] decoderConfig=${!!s?.decoderConfig} audioConfig=${!!s?.audioConfig} slot=${s?.slot}`);
    if (s?.decoderConfig) socket.emit('decoder-config', { streamerId, config: s.decoderConfig, slot: s.slot });
    if (s?.audioConfig) socket.emit('audio-config', { streamerId, config: s.audioConfig });
    requestKeyframe(room, streamerId);
  });

  // O decoder do viewer travou/errou e precisa de um ponto de partida novo.
  socket.on('request-keyframe', ({ streamerId }) => {
    if (!roomId) return;
    const room = getRoom(roomId);
    const w = room.watchers.get(socket.id);
    if (w && w.streamerId === streamerId) w.needsKey = true;
    requestKeyframe(room, streamerId);
  });

  socket.on('disconnect', () => {
    if (!roomId) return;
    const room = getRoom(roomId);
    const prev = room.watchers.get(socket.id);
    if (prev) {
      removeViewer(prev.streamerId, socket.id);
      room.watchers.delete(socket.id);
      broadcastViewerCount(room, prev.streamerId);
    }
    room.participants.delete(socket.id);
    socket.to(roomId).emit('user-left', { socketId: socket.id });
    if (room.participants.size === 0 && room.streamers.size === 0) rooms.delete(roomId);
  });
});

const PORT = process.env.PORT || 3001;
httpServer.listen(PORT, () => console.log(`ShareScreen server on port ${PORT}`));
