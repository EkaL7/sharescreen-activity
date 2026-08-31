import { DiscordSDK } from '@discord/embedded-app-sdk';
import { io } from 'socket.io-client';
import './style.css';

const CLIENT_ID = import.meta.env.VITE_DISCORD_CLIENT_ID;
const STREAM_BASE = `${window.location.origin}/stream`;

// Protocolo binário: [1B slot][1B tipo][8B ts][8B relógio][payload]
const TIPO_KEYFRAME = 1;
const TIPO_DELTA = 2;
const TIPO_AUDIO = 3;

const hasWebCodecs = Boolean(window.VideoDecoder && window.EncodedVideoChunk);

let discordSdk, socket, currentUser = null;
const participants = new Map();
const streamers = new Map();          // streamerId → {username, fonte}
const thumbUrls = new Map();          // streamerId → objectURL da miniatura
let watchingId = null;
let expectedSlot = null;

// ── Decoder de vídeo ──
let videoDecoder = null;
let pendingConfig = null;
let gotKeyframe = false;
let decodeErrors = 0;
let canvas = null, canvasCtx = null;

// ── Áudio ──
let audioDecoder = null;
let audioCtx = null;
let gainNode = null;
let audioNextTime = 0;
let volume = 1;
let muted = false;
try {
  const saved = Number(localStorage.getItem('ss-volume'));
  if (!Number.isNaN(saved) && saved >= 0 && saved <= 1) volume = saved;
} catch {}

// ── Stats ──
let recvBytes = 0, decFrames = 0, lastW = 0, lastH = 0;
let statsTimer = null;

const esc = s => String(s).replace(/[&<>"']/g, c =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

function toU8(data) {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  return null;
}

async function init() {
  const app = document.getElementById('app');
  app.innerHTML = '<div class="loading"><div class="spinner"></div><p>Conectando ao Discord...</p></div>';

  try {
    discordSdk = new DiscordSDK(CLIENT_ID);
    await discordSdk.ready();

    const { code } = await discordSdk.commands.authorize({
      client_id: CLIENT_ID, response_type: 'code', state: '', prompt: 'none',
      scope: ['identify', 'guilds'],
    });

    const res = await fetch('/api/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code }),
    });
    const { access_token } = await res.json();
    const auth = await discordSdk.commands.authenticate({ access_token });
    currentUser = auth.user;

    setupSocket();
    renderUI();
  } catch (err) {
    app.innerHTML = `<div class="loading"><p>Erro: ${esc(err.message)}</p></div>`;
  }
}

function setupSocket() {
  socket = io({ transports: ['websocket', 'polling'] });

  socket.on('connect', () => {
    const avatarUrl = currentUser.avatar
      ? `https://cdn.discordapp.com/avatars/${currentUser.id}/${currentUser.avatar}.png?size=64`
      : 'https://cdn.discordapp.com/embed/avatars/0.png';
    socket.emit('join-room', {
      roomId: discordSdk.instanceId,
      userId: currentUser.id,
      username: currentUser.global_name || currentUser.username,
      avatarUrl,
    });
  });

  socket.on('room-state', ({ participants: parts, streamers: strs }) => {
    participants.clear();
    parts.forEach(p => participants.set(p.socketId, p));
    // Reconexão: streamers antigos morreram junto com a conexão do servidor.
    streamers.clear();
    strs.forEach(s => {
      streamers.set(s.socketId, { username: s.username, fonte: s.fonte });
      if (s.thumbnail) setThumb(s.socketId, s.thumbnail);
    });
    if (watchingId && !streamers.has(watchingId)) { watchingId = null; destroyDecoder(); showPlaceholder(); }
    renderParticipants();
    renderStreamers();
    if (streamers.size > 0 && !watchingId) watchStream(streamers.keys().next().value);
  });

  socket.on('user-joined', user => { participants.set(user.socketId, user); renderParticipants(); });
  socket.on('user-left', ({ socketId }) => { participants.delete(socketId); renderParticipants(); });

  socket.on('streamer-added', ({ socketId, username, fonte }) => {
    streamers.set(socketId, { username, fonte: fonte || 'tela' });
    renderStreamers();
    if (!watchingId) watchStream(socketId);
  });

  socket.on('streamer-removed', ({ socketId }) => {
    streamers.delete(socketId);
    const url = thumbUrls.get(socketId);
    if (url) { URL.revokeObjectURL(url); thumbUrls.delete(socketId); }
    if (watchingId === socketId) {
      watchingId = null;
      destroyDecoder();
      if (streamers.size > 0) watchStream(streamers.keys().next().value);
      else showPlaceholder();
    }
    renderStreamers();
  });

  socket.on('decoder-config', ({ streamerId, config, slot }) => {
    if (streamerId !== watchingId) return;
    expectedSlot = slot ?? null;
    initDecoder(config);
    socket.emit('request-keyframe', { streamerId });
  });

  socket.on('audio-config', ({ streamerId, config }) => {
    if (streamerId !== watchingId) return;
    initAudioDecoder(config);
  });

  socket.on('stream-thumbnail', ({ streamerId, data }) => {
    setThumb(streamerId, data);
    // Fallback sem WebCodecs: a miniatura vira o vídeo principal (1 fps).
    if (!hasWebCodecs && streamerId === watchingId) {
      const img = document.getElementById('stream-fallback');
      const u8 = toU8(data);
      if (img && u8) {
        const url = URL.createObjectURL(new Blob([u8], { type: 'image/jpeg' }));
        img.onload = () => URL.revokeObjectURL(url);
        img.src = url;
        img.style.display = 'block';
        hideEl('placeholder');
        showOverlay();
      }
    }
  });

  socket.on('stream-data', (data) => {
    if (!watchingId) return;
    const u8 = toU8(data);
    if (!u8 || u8.byteLength < 18) return;

    const view = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
    const slot = view.getUint8(0);
    const tipo = view.getUint8(1);
    const timestamp = view.getFloat64(2);

    // Frames atrasados de um streamer anterior (troca de stream) são ignorados.
    if (expectedSlot != null && slot !== expectedSlot) return;

    const payload = u8.subarray(18);
    recvBytes += u8.byteLength;

    if (tipo === TIPO_AUDIO) { decodeAudio(payload, timestamp); return; }
    if (!videoDecoder || videoDecoder.state !== 'configured') return;

    const isKey = tipo === TIPO_KEYFRAME;
    if (!gotKeyframe && !isKey) return;
    gotKeyframe = true;

    try {
      videoDecoder.decode(new EncodedVideoChunk({
        type: isKey ? 'key' : 'delta',
        timestamp,
        data: payload,
      }));
      decodeErrors = 0;
    } catch (err) {
      console.warn('[decode]', err.message);
      if (++decodeErrors >= 3) recoverDecoder();
    }
  });
}

// ─────────────────────────────────────────── Vídeo (WebCodecs)

function initDecoder(config) {
  destroyVideoDecoder();
  if (!hasWebCodecs) return;

  pendingConfig = config;
  gotKeyframe = false;
  decodeErrors = 0;

  const decoderConfig = {
    codec: config.codec,
    codedWidth: config.codedWidth,
    codedHeight: config.codedHeight,
    optimizeForLatency: true,
  };
  if (config.description) {
    const bin = atob(config.description);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    decoderConfig.description = bytes.buffer;
  }

  canvas = document.getElementById('stream-canvas');
  canvasCtx = canvas?.getContext('2d', { alpha: false, desynchronized: true });

  videoDecoder = new VideoDecoder({
    output: onFrame,
    error: err => { console.warn('[VideoDecoder]', err.message); recoverDecoder(); },
  });

  VideoDecoder.isConfigSupported(decoderConfig).then(({ supported }) => {
    if (!videoDecoder) return;
    if (supported) {
      videoDecoder.configure(decoderConfig);
      if (canvas) canvas.style.display = 'block';
    } else {
      console.warn('[decoder] codec não suportado:', config.codec);
      showToast('Codec não suportado neste dispositivo');
    }
  }).catch(err => console.warn('[decoder]', err.message));
}

function onFrame(frame) {
  if (!canvas || !canvasCtx) { frame.close(); return; }
  if (canvas.width !== frame.displayWidth || canvas.height !== frame.displayHeight) {
    canvas.width = frame.displayWidth;
    canvas.height = frame.displayHeight;
  }
  canvasCtx.drawImage(frame, 0, 0);
  lastW = frame.displayWidth; lastH = frame.displayHeight;
  decFrames++;
  frame.close();

  hideEl('placeholder');
  showOverlay();
}

/** Decoder morreu ou está corrompido: reconfigura e pede keyframe novo. */
function recoverDecoder() {
  if (!watchingId || !pendingConfig) return;
  decodeErrors = 0;
  try { initDecoder(pendingConfig); } catch {}
  socket.emit('request-keyframe', { streamerId: watchingId });
}

function destroyVideoDecoder() {
  if (videoDecoder && videoDecoder.state !== 'closed') {
    try { videoDecoder.close(); } catch {}
  }
  videoDecoder = null;
  gotKeyframe = false;
}

function destroyDecoder() {
  destroyVideoDecoder();
  pendingConfig = null;
  expectedSlot = null;
  if (audioDecoder && audioDecoder.state !== 'closed') {
    try { audioDecoder.close(); } catch {}
  }
  audioDecoder = null;
  audioNextTime = 0;
}

// ─────────────────────────────────────────── Áudio

function ensureAudioGraph() {
  if (audioCtx) return;
  audioCtx = new AudioContext();
  gainNode = audioCtx.createGain();
  gainNode.gain.value = muted ? 0 : volume;
  gainNode.connect(audioCtx.destination);
}

function initAudioDecoder(config) {
  if (!window.AudioDecoder) return;
  ensureAudioGraph();
  audioCtx.resume().catch(() => {});

  if (audioDecoder && audioDecoder.state !== 'closed') {
    try { audioDecoder.close(); } catch {}
  }
  audioNextTime = 0;

  audioDecoder = new AudioDecoder({
    output: onAudioFrame,
    error: err => console.warn('[AudioDecoder]', err.message),
  });
  audioDecoder.configure({
    codec: config.codec || 'opus',
    sampleRate: config.sampleRate || 48000,
    numberOfChannels: config.numberOfChannels || 2,
  });
}

/**
 * Reprodução agendada: cada pacote entra na fila no instante certo, sem
 * estalos. Se o atraso acumular (> 0.5s), o relógio é resetado — preferimos
 * um pulo a um eco crescente.
 */
function onAudioFrame(ad) {
  if (!audioCtx) { ad.close(); return; }
  const channels = ad.numberOfChannels, frames = ad.numberOfFrames, sr = ad.sampleRate;

  let buffer;
  try {
    buffer = audioCtx.createBuffer(channels, frames, sr);
    for (let ch = 0; ch < channels; ch++) {
      ad.copyTo(buffer.getChannelData(ch), { planeIndex: ch, format: 'f32-planar' });
    }
  } catch { ad.close(); return; }
  ad.close();

  const now = audioCtx.currentTime;
  if (audioNextTime < now + 0.02) audioNextTime = now + 0.02;
  if (audioNextTime > now + 0.5) audioNextTime = now + 0.05;

  const src = audioCtx.createBufferSource();
  src.buffer = buffer;
  src.connect(gainNode);
  src.start(audioNextTime);
  audioNextTime += buffer.duration;
}

function decodeAudio(payload, timestamp) {
  if (!audioDecoder || audioDecoder.state !== 'configured') return;
  try {
    audioDecoder.decode(new EncodedAudioChunk({ type: 'key', timestamp, data: payload }));
  } catch {}
}

function applyVolume() {
  if (gainNode) gainNode.gain.value = muted ? 0 : volume;
  const btn = document.getElementById('btn-mute');
  if (btn) btn.innerHTML = muted || volume === 0 ? ICON_MUTED : ICON_SOUND;
  const slider = document.getElementById('vol-slider');
  if (slider) slider.value = muted ? 0 : Math.round(volume * 100);
}

// ─────────────────────────────────────────── Watch / UI state

function watchStream(streamerId) {
  watchingId = streamerId;
  destroyDecoder();
  socket.emit('watch-stream', { streamerId });

  const s = streamers.get(streamerId);
  const name = s ? s.username : 'Streamer';
  const ph = document.getElementById('placeholder');
  if (ph) {
    ph.innerHTML = `<div class="spinner"></div><p>${esc(name)} — aguardando...</p>`;
    ph.style.display = 'flex';
  }
  hideEl('stream-canvas');
  hideEl('stream-fallback');
  hideEl('viewer-overlay');
  const nameEl = document.getElementById('overlay-name');
  if (nameEl) nameEl.textContent = name;

  recvBytes = 0; decFrames = 0; lastW = 0; lastH = 0;
  clearInterval(statsTimer);
  statsTimer = setInterval(updateViewerStats, 1000);

  if (!hasWebCodecs) {
    showToast('Sem WebCodecs neste dispositivo — mostrando prévia de baixa qualidade');
  }
  renderStreamers();
}

function updateViewerStats() {
  const el = document.getElementById('overlay-stats');
  if (!el) return;
  const mbps = (recvBytes * 8 / 1e6).toFixed(1);
  el.textContent = lastW ? `${lastW}×${lastH} · ${decFrames} fps · ${mbps} Mb/s` : '';
  recvBytes = 0; decFrames = 0;
}

function stopWatching() {
  watchingId = null;
  destroyDecoder();
  clearInterval(statsTimer);
  socket.emit('watch-stream', { streamerId: null });
  showPlaceholder();
  hideEl('viewer-overlay');
  renderStreamers();
}

function showPlaceholder() {
  const ph = document.getElementById('placeholder');
  if (ph) {
    ph.innerHTML = `<div class="ph-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="56" height="56"><rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/></svg></div><p>Ninguém está compartilhando a tela</p><p class="sub">Clique no botão abaixo para transmitir</p>`;
    ph.style.display = 'flex';
  }
  const c = document.getElementById('stream-canvas');
  if (c) { c.style.display = 'none'; c.width = 0; c.height = 0; }
  hideEl('stream-fallback');
}

function hideEl(id) { const el = document.getElementById(id); if (el) el.style.display = 'none'; }

function showOverlay() {
  const overlay = document.getElementById('viewer-overlay');
  if (overlay && overlay.style.display === 'none') overlay.style.display = 'flex';
}

function setThumb(streamerId, data) {
  const u8 = toU8(data);
  if (!u8 || !u8.byteLength) return;
  const old = thumbUrls.get(streamerId);
  if (old) URL.revokeObjectURL(old);
  const url = URL.createObjectURL(new Blob([u8], { type: 'image/jpeg' }));
  thumbUrls.set(streamerId, url);
  const img = document.getElementById('thumb-' + streamerId);
  if (img) img.src = url;
}

async function openStreamPage() {
  const username = currentUser.global_name || currentUser.username;
  const url = `${STREAM_BASE}?room=${discordSdk.instanceId}&user=${encodeURIComponent(username)}`;
  try { await discordSdk.commands.openExternalLink({ url }); }
  catch { showToast('Não foi possível abrir o link'); }
}

async function toggleFullscreen() {
  const area = document.getElementById('video-area');
  if (document.fullscreenElement) { document.exitFullscreen().catch(() => {}); return; }
  try { await area.requestFullscreen(); }
  catch { document.querySelector('.main')?.classList.toggle('theater'); }
}

// ─────────────────────────────────────────── Render

const ICON_SOUND = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18"><path d="M11 5L6 9H2v6h4l5 4V5zM15.5 8.5a5 5 0 010 7M19 5a9 9 0 010 14"/></svg>';
const ICON_MUTED = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18"><path d="M11 5L6 9H2v6h4l5 4V5zM23 9l-6 6M17 9l6 6"/></svg>';

function renderUI() {
  document.getElementById('app').innerHTML = `
    <div class="main">
      <div class="video-area" id="video-area">
        <div class="placeholder" id="placeholder">
          <div class="ph-icon">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="56" height="56">
              <rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/>
            </svg>
          </div>
          <p>Ninguém está compartilhando a tela</p>
          <p class="sub">Clique no botão para transmitir</p>
        </div>
        <button id="share-btn-center" class="btn-share-center">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="22" height="22">
            <rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/>
          </svg>
          Compartilhar Tela
        </button>
        <canvas id="stream-canvas" style="display:none"></canvas>
        <img id="stream-fallback" alt="" style="display:none" />
        <div class="viewer-overlay" id="viewer-overlay" style="display:none">
          <div class="overlay-top">
            <div class="overlay-info">
              <span class="overlay-live-dot"></span>
              <span id="overlay-name"></span>
              <span class="overlay-stats" id="overlay-stats"></span>
            </div>
            <div class="overlay-controls">
              <button class="ov-btn" id="btn-switch" title="Trocar transmissão">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18"><path d="M7 16V4m0 0L3 8m4-4l4 4M17 8v12m0 0l4-4m-4 4l-4-4"/></svg>
              </button>
              <button class="ov-btn" id="btn-close" title="Fechar">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18"><path d="M18 6L6 18M6 6l12 12"/></svg>
              </button>
            </div>
          </div>
          <div class="overlay-bottom">
            <button class="ov-btn" id="btn-mute" title="Mudo">${ICON_SOUND}</button>
            <input type="range" id="vol-slider" class="vol-slider" min="0" max="100" value="${Math.round(volume * 100)}" />
            <div style="flex:1"></div>
            <button class="ov-btn" id="btn-fs" title="Tela cheia">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18"><path d="M8 3H5a2 2 0 00-2 2v3m18 0V5a2 2 0 00-2-2h-3m0 18h3a2 2 0 002-2v-3M3 16v3a2 2 0 002 2h3"/></svg>
            </button>
          </div>
        </div>
      </div>
      <div class="streamers-bar" id="streamers-bar"></div>
      <div class="toolbar">
        <div class="participants-bar" id="pbar"></div>
        <button id="share-btn" class="btn-share">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="20" height="20">
            <rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/>
          </svg>
          <span>Compartilhar Tela</span>
        </button>
      </div>
    </div>`;

  document.getElementById('share-btn').addEventListener('click', openStreamPage);
  document.getElementById('share-btn-center').addEventListener('click', openStreamPage);
  document.getElementById('btn-close').addEventListener('click', stopWatching);
  document.getElementById('btn-fs').addEventListener('click', toggleFullscreen);
  document.getElementById('btn-switch').addEventListener('click', () => {
    if (streamers.size <= 1) return;
    const ids = [...streamers.keys()];
    const idx = ids.indexOf(watchingId);
    watchStream(ids[(idx + 1) % ids.length]);
  });

  document.getElementById('btn-mute').addEventListener('click', () => {
    muted = !muted;
    applyVolume();
    audioCtx?.resume().catch(() => {});
  });
  document.getElementById('vol-slider').addEventListener('input', (e) => {
    volume = Number(e.target.value) / 100;
    muted = false;
    applyVolume();
    try { localStorage.setItem('ss-volume', String(volume)); } catch {}
    audioCtx?.resume().catch(() => {});
  });

  // Autoplay: o primeiro gesto em qualquer lugar libera o AudioContext.
  document.addEventListener('click', () => { audioCtx?.resume().catch(() => {}); }, { capture: true });

  applyVolume();
  renderParticipants();
  renderStreamers();
}

function renderStreamers() {
  const bar = document.getElementById('streamers-bar');
  if (!bar) return;
  const centerBtn = document.getElementById('share-btn-center');
  if (centerBtn) centerBtn.style.display = (streamers.size > 0 || watchingId) ? 'none' : 'flex';
  if (streamers.size === 0) { bar.innerHTML = ''; return; }
  bar.innerHTML = '<div class="streamers-label">Transmissões ao vivo:</div>';
  streamers.forEach((s, sid) => {
    const active = sid === watchingId ? 'active' : '';
    const badge = s.fonte === 'camera'
      ? '<span class="fonte-badge cam">CAM</span>'
      : '<span class="fonte-badge tela">TELA</span>';
    const thumbSrc = thumbUrls.get(sid) || '';
    bar.innerHTML += `
      <div class="streamer-card ${active}" data-sid="${esc(sid)}">
        <div class="streamer-thumb"><img id="thumb-${esc(sid)}" src="${esc(thumbSrc)}" alt="" /><div class="live-dot"></div></div>
        ${badge}
        <span class="streamer-name">${esc(s.username)}</span>
      </div>`;
  });
  bar.querySelectorAll('.streamer-card').forEach(card => {
    card.addEventListener('click', () => {
      const sid = card.dataset.sid;
      if (sid && sid !== watchingId) watchStream(sid);
    });
  });
}

function renderParticipants() {
  const bar = document.getElementById('pbar');
  if (!bar) return;
  bar.innerHTML = '';
  participants.forEach(p => {
    bar.innerHTML += `<div class="participant">
      <img src="${esc(p.avatarUrl)}" alt=""/><span>${esc(p.username)}</span>
    </div>`;
  });
}

function showToast(msg) {
  const t = document.createElement('div');
  t.className = 'toast';
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(() => t.remove(), 3500);
}

init();
