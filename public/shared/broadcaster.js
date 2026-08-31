/**
 * Pipeline de transmissão: captura → codifica → envia.
 *
 * WebCodecs codifica quadro a quadro e envia direto via WebSocket binário.
 * Sem WebRTC (Activity não tem) e sem MediaRecorder (latência alta).
 */

function h264Codec(width, height, framerate) {
  const macroblocks = Math.ceil(width / 16) * Math.ceil(height / 16);
  const perSecond = macroblocks * framerate;
  const levels = [
    { maxFrame: 1620, maxSecond: 40_500, hex: '1E' },
    { maxFrame: 3600, maxSecond: 108_000, hex: '1F' },
    { maxFrame: 5120, maxSecond: 216_000, hex: '20' },
    { maxFrame: 8192, maxSecond: 245_760, hex: '28' },
    { maxFrame: 8704, maxSecond: 522_240, hex: '2A' },
  ];
  const level = levels.find(l => macroblocks <= l.maxFrame && perSecond <= l.maxSecond);
  return `avc1.42E0${level?.hex ?? '2A'}`;
}

function candidatesFor(width, height, framerate) {
  const avc = h264Codec(width, height, framerate);
  return [
    { codec: avc, avc: { format: 'annexb' } },
    { codec: avc },
    { codec: 'vp8' },
    { codec: 'vp09.00.10.08' },
  ];
}

const codecForSize = (current, w, h, fps) =>
  current?.startsWith('avc1.') ? h264Codec(w, h, fps) : current;

const KEYFRAME_EVERY_MS = 3000;
const TIPO_KEYFRAME = 1;
const TIPO_DELTA = 2;
const TIPO_AUDIO = 3;
const TIPO_THUMB = 4;
const AUDIO_BITRATE = 96_000;
const MAX_W = 1920;
const MAX_H = 1080;
const OUTPUT_LIMITS = [
  { width: 1920, height: 1080 },
  { width: 1600, height: 900 },
  { width: 1280, height: 720 },
];
const MIN_WS_BUFFER = 128 * 1024;

const even = n => Math.max(2, n - (n % 2));
const screenContentHint = fps => (fps >= 50 ? 'motion' : 'text');

const captureConstraints = (fps, maxW = MAX_W, maxH = MAX_H) => ({
  width: { ideal: maxW, max: maxW },
  height: { ideal: maxH, max: maxH },
  frameRate: { ideal: fps, max: fps },
  resizeMode: 'crop-and-scale',
});

function fitWithin(w, h, maxW = MAX_W, maxH = MAX_H) {
  const scale = Math.min(1, maxW / w, maxH / h);
  return { width: even(Math.round(w * scale)), height: even(Math.round(h * scale)) };
}

export function restricoesDeSom() {
  const c = { echoCancellation: false, noiseSuppression: false, autoGainControl: false };
  if (navigator.mediaDevices.getSupportedConstraints?.().restrictOwnAudio) {
    c.restrictOwnAudio = true;
  }
  return c;
}

export function opcoesTela({ fps = 30, comSom = false, video } = {}) {
  const opts = {
    video: video ?? { displaySurface: 'window', ...captureConstraints(fps) },
    audio: comSom ? restricoesDeSom() : false,
    monitorTypeSurfaces: 'include',
    preferCurrentTab: false,
    selfBrowserSurface: 'exclude',
    surfaceSwitching: 'exclude',
  };
  if (comSom) {
    opts.windowAudio = 'window';
    opts.systemAudio = 'include';
    opts.audioSelection = 'preferred';
  }
  return opts;
}

export function supportError() {
  if (!window.VideoEncoder || !window.VideoFrame || !window.EncodedVideoChunk) {
    return 'Este navegador não tem WebCodecs. Use Chrome, Edge, Brave ou Firefox recente.';
  }
  return null;
}

export function fonteIndisponivel(fonte) {
  if (fonte === 'camera') {
    return navigator.mediaDevices?.getUserMedia ? null : 'Este navegador não permite acesso à câmera.';
  }
  return navigator.mediaDevices?.getDisplayMedia
    ? null
    : 'Este navegador não permite captura de tela. Use um desktop.';
}

export function createBroadcaster({
  wsUrl, bitrate, fps, audio = false, fonte = 'tela',
  streamPronto = null, deviceId = null,
  onStatus, onStats, onEnd, onAviso,
}) {
  let ws = null, stream = null, encoder = null, reader = null;
  let audioEncoder = null, audioReader = null;
  let video = null, frameClock = null, config = null, stage = null, stageCtx = null;

  let running = false, mySlot = 0, wantKeyframe = true, lastKeyframeAt = 0;
  let srcW = 0, srcH = 0, startedAt = 0, bytes = 0, frames = 0, viewers = 0;
  let statsTimer = null, capturedFrames = 0, encoderDrops = 0, networkDrops = 0;
  let outputLimitIndex = 0, networkPressureWindows = 0, displaySurface = null;

  async function start() {
    stream = streamPronto ?? (fonte === 'camera' ? await capturarCamera() : await capturarTela());

    const track = stream.getVideoTracks()[0];
    displaySurface = track.getSettings?.().displaySurface ?? null;
    track.contentHint = fonte === 'camera' ? 'motion' : screenContentHint(fps);
    track.addEventListener('ended', () =>
      stop(fonte === 'camera' ? 'A câmera foi desligada.' : 'Você parou o compartilhamento.'));

    if (fonte === 'tela') await track.applyConstraints?.(captureConstraints(fps)).catch(() => {});

    const s = track.getSettings();
    const target = fitWithin(s.width ?? 1280, s.height ?? 720);

    config = await pickConfig(target.width, target.height);
    if (!config) { cleanup(); throw new Error('Nenhum codec de vídeo suportado.'); }

    await connect();

    encoder = new VideoEncoder({
      output: onEncoded,
      error: err => stop(`Erro no encoder: ${err.message}`),
    });
    encoder.configure(config);

    ws.send(JSON.stringify({ type: 'start' }));

    running = true;
    wantKeyframe = true;
    lastKeyframeAt = 0;
    srcW = srcH = 0;
    startedAt = Date.now();

    onStatus?.({
      codec: config.codec, width: config.width, height: config.height,
      direct: Boolean(window.MediaStreamTrackProcessor),
      captureFps: s.frameRate ?? null,
    });

    statsTimer = setInterval(() => {
      adaptVideoQuality();
      adaptNetworkQuality();
      onStats?.({
        viewers, fps: frames, captureFps: capturedFrames,
        droppedFrames: encoderDrops + networkDrops, encoderDrops, networkDrops,
        encoderQueue: encoder?.encodeQueueSize ?? 0,
        networkBuffer: ws?.bufferedAmount ?? 0,
        mbps: (bytes * 8) / 1e6,
        seconds: Math.floor((Date.now() - startedAt) / 1000),
      });
      bytes = frames = capturedFrames = encoderDrops = networkDrops = 0;
    }, 1000);

    pump(track);
    const audioTrack = prepararSom(track, stream);
    if (audioTrack) pumpAudio(audioTrack);

    return stream;
  }

  function capturarTela() {
    return navigator.mediaDevices.getDisplayMedia(opcoesTela({ fps, comSom: audio }));
  }

  function capturarCamera() {
    return navigator.mediaDevices.getUserMedia({
      video: {
        ...(deviceId ? { deviceId: { exact: deviceId } } : {}),
        width: { ideal: 1280 }, height: { ideal: 720 },
        frameRate: { ideal: fps, max: fps },
      },
      audio: false,
    });
  }

  function prepararSom(videoTrack, capturado) {
    if (!audio) return null;
    const faixa = capturado.getAudioTracks()[0];
    const superficie = videoTrack.getSettings?.().displaySurface;
    if (!faixa) { onAviso?.('A captura iniciou sem áudio.'); return null; }

    if (superficie === 'browser') return faixa;
    if (superficie === 'window' && navigator.mediaDevices.getSupportedConstraints?.().restrictOwnAudio) return faixa;

    if (superficie === 'monitor') {
      onAviso?.('Áudio da tela inteira ligado — inclui todos os sons, inclusive Discord.');
      return faixa;
    }

    faixa.stop();
    capturado.removeTrack(faixa);
    onAviso?.('Áudio bloqueado: traria o Discord junto. Transmitindo sem som.');
    return null;
  }

  async function pumpAudio(track) {
    if (!window.AudioEncoder || !window.MediaStreamTrackProcessor) {
      track.stop();
      onAviso?.('Navegador não suporta envio de áudio. Use Chrome/Edge/Brave.');
      return;
    }
    const s = track.getSettings();
    const sampleRate = s.sampleRate || 48_000;
    const numberOfChannels = Math.min(2, s.channelCount || 2);

    try {
      audioEncoder = new AudioEncoder({
        output: onAudioEncoded,
        error: err => console.warn('[audio encoder]', err.message),
      });
      audioEncoder.configure({ codec: 'opus', sampleRate, numberOfChannels, bitrate: AUDIO_BITRATE });
    } catch { audioEncoder = null; return; }

    ws?.send(JSON.stringify({ type: 'audio-config', config: { codec: 'opus', sampleRate, numberOfChannels } }));

    audioReader = new MediaStreamTrackProcessor({ track }).readable.getReader();
    while (running) {
      let dados;
      try { const { done, value } = await audioReader.read(); if (done) break; dados = value; }
      catch { break; }
      if (audioEncoder?.state === 'configured') {
        try { audioEncoder.encode(dados); } catch {}
      }
      dados.close();
    }
  }

  function onAudioEncoded(chunk) {
    if (ws?.readyState !== WebSocket.OPEN) return;
    const data = new Uint8Array(chunk.byteLength);
    chunk.copyTo(data);
    ws.send(empacotar(TIPO_AUDIO, chunk.timestamp ?? 0, data));
    bytes += 18 + data.byteLength;
  }

  async function pickConfig(width, height) {
    for (const hardware of [true, false]) {
      for (const realtime of [true, false]) {
        for (const candidate of candidatesFor(width, height, fps)) {
          const cfg = { ...candidate, width, height, bitrate, framerate: fps };
          if (hardware) cfg.hardwareAcceleration = 'prefer-hardware';
          if (realtime) cfg.latencyMode = 'realtime';
          try { const { supported } = await VideoEncoder.isConfigSupported(cfg); if (supported) return cfg; }
          catch {}
        }
      }
    }
    return null;
  }

  function pump(track) {
    if (window.MediaStreamTrackProcessor) pumpDirect(track);
    else pumpViaVideo();
  }

  async function pumpDirect(track) {
    reader = new MediaStreamTrackProcessor({ track }).readable.getReader();
    while (running) {
      let frame;
      try { const { done, value } = await reader.read(); if (done) break; frame = value; }
      catch { break; }
      if (!encodeFrame(frame)) break;
    }
  }

  function pumpViaVideo() {
    video = document.createElement('video');
    video.muted = true; video.playsInline = true; video.srcObject = stream;
    Object.assign(video.style, { position:'fixed',left:'-9999px',width:'2px',height:'2px',opacity:'0' });
    document.body.append(video);
    video.play()?.catch?.(() => {});

    const t0 = performance.now();
    const hasRvfc = typeof video.requestVideoFrameCallback === 'function';
    let lastAt = 0;

    const schedule = () => { if (running) { if (hasRvfc) video.requestVideoFrameCallback(tick); else requestAnimationFrame(tick); } };
    const rescheduleFallback = () => { if (!frameClock) schedule(); };

    const tick = () => {
      if (!running) return;
      if (video.paused) video.play()?.catch?.(() => {});
      if (video.readyState < 2 || !video.videoWidth) return rescheduleFallback();
      const now = performance.now();
      if (!frameClock && !hasRvfc && now - lastAt < 1000 / (fps + 2)) return rescheduleFallback();
      lastAt = now;

      let frame;
      try { frame = new VideoFrame(video, { timestamp: (now - t0) * 1000 }); }
      catch { return rescheduleFallback(); }
      encodeFrame(frame);
      rescheduleFallback();
    };

    try {
      frameClock = new Worker(new URL('./frame-worker.js', import.meta.url), { type: 'module' });
      frameClock.onmessage = tick;
      frameClock.postMessage({ type: 'start', fps });
    } catch { frameClock = null; schedule(); }
  }

  function encodeFrame(frame) {
    if (!running || encoder?.state !== 'configured') { frame.close(); return false; }
    capturedFrames++;

    const maxBuffered = Math.max(MIN_WS_BUFFER, bitrate / 8 / 4);
    if ((ws?.bufferedAmount ?? 0) > maxBuffered) { networkDrops++; frame.close(); return true; }
    if (encoder.encodeQueueSize > 2) { encoderDrops++; frame.close(); return true; }

    const timestamp = frame.timestamp ?? performance.now() * 1000;
    syncSize(frame);

    const now = Date.now();
    if (now - lastKeyframeAt > KEYFRAME_EVERY_MS) wantKeyframe = true;

    let out = frame;
    if (stage) {
      stageCtx.drawImage(frame, 0, 0, stage.width, stage.height);
      frame.close();
      out = new VideoFrame(stage, { timestamp });
    }

    try {
      encoder.encode(out, { keyFrame: wantKeyframe });
      if (wantKeyframe) { lastKeyframeAt = now; wantKeyframe = false; }
    } catch (err) { console.error('[encode]', err); }
    out.close();
    return true;
  }

  function adaptVideoQuality() {
    const ready = capturedFrames >= Math.max(10, Math.round(fps / 4));
    const saturated = ready && encoderDrops / Math.max(1, capturedFrames) >= 0.2;
    const slowCapture = !window.MediaStreamTrackProcessor && capturedFrames >= 5 && capturedFrames < Math.round(fps * 0.7);
    if (!saturated && !slowCapture) return;

    const source = stream?.getVideoTracks()[0]?.getSettings?.() ?? {};
    const sw = source.width || srcW || config?.width;
    const sh = source.height || srcH || config?.height;
    const currentPx = (config?.width ?? 0) * (config?.height ?? 0);

    for (let next = outputLimitIndex + 1; next < OUTPUT_LIMITS.length; next++) {
      const limit = OUTPUT_LIMITS[next];
      const target = fitWithin(sw, sh, limit.width, limit.height);
      if (target.width * target.height >= currentPx) continue;

      outputLimitIndex = next;
      try { encoder.reset(); } catch {}
      config = { ...config, ...target, codec: codecForSize(config.codec, target.width, target.height, fps) };
      encoder.configure(config);
      srcW = sw; srcH = sh;
      prepareStage(sw, sh, target);
      wantKeyframe = true;
      onStatus?.({ codec: config.codec, width: config.width, height: config.height, direct: Boolean(window.MediaStreamTrackProcessor) });
      onAviso?.(`Resolução ajustada para ${target.width}×${target.height}.`);
      break;
    }
  }

  function adaptNetworkQuality() {
    const congested = capturedFrames >= 10 && networkDrops / capturedFrames >= 0.2;
    networkPressureWindows = congested ? networkPressureWindows + 1 : 0;
    if (networkPressureWindows < 2 || bitrate <= 1_200_000) return;
    const next = Math.max(1_200_000, Math.floor((bitrate * 0.75) / 100_000) * 100_000);
    if (next >= bitrate) return;
    bitrate = next;
    networkPressureWindows = 0;
    config = { ...config, bitrate };
    encoder.configure(config);
    wantKeyframe = true;
    onAviso?.(`Bitrate ajustado para ${(bitrate / 1e6).toFixed(1)} Mb/s.`);
  }

  function syncSize(frame) {
    const sw = frame.displayWidth, sh = frame.displayHeight;
    if (!sw || !sh || (sw === srcW && sh === srcH)) return;
    srcW = sw; srcH = sh;
    const limit = OUTPUT_LIMITS[outputLimitIndex];
    const target = fitWithin(sw, sh, limit.width, limit.height);
    if (target.width !== config.width || target.height !== config.height) {
      config = { ...config, ...target, codec: codecForSize(config.codec, target.width, target.height, fps) };
      encoder.configure(config);
      wantKeyframe = true;
      onStatus?.({ codec: config.codec, width: config.width, height: config.height, direct: Boolean(window.MediaStreamTrackProcessor) });
    }
    prepareStage(sw, sh, target);
  }

  function prepareStage(sw, sh, target) {
    if (target.width === sw && target.height === sh) { stage = null; stageCtx = null; }
    else {
      stage = document.createElement('canvas');
      stage.width = target.width; stage.height = target.height;
      stageCtx = stage.getContext('2d', { alpha: false, desynchronized: true });
    }
  }

  function onEncoded(chunk, metadata) {
    if (ws?.readyState !== WebSocket.OPEN) return;
    if (metadata?.decoderConfig) {
      ws.send(JSON.stringify({ type: 'config', config: serializeConfig(metadata.decoderConfig) }));
    }
    const data = new Uint8Array(chunk.byteLength);
    chunk.copyTo(data);
    const buf = empacotar(chunk.type === 'key' ? TIPO_KEYFRAME : TIPO_DELTA, chunk.timestamp ?? 0, data);
    ws.send(buf);
    bytes += buf.byteLength;
    frames++;
  }

  function empacotar(tipo, timestamp, data) {
    const buf = new ArrayBuffer(18 + data.byteLength);
    const view = new DataView(buf);
    view.setUint8(0, mySlot);
    view.setUint8(1, tipo);
    view.setFloat64(2, timestamp);
    view.setFloat64(10, Date.now());
    new Uint8Array(buf, 18).set(data);
    return buf;
  }

  function serializeConfig(dc) {
    const out = { codec: dc.codec, codedWidth: dc.codedWidth, codedHeight: dc.codedHeight };
    if (dc.description) {
      const b = new Uint8Array(dc.description instanceof ArrayBuffer ? dc.description : dc.description.buffer);
      let bin = ''; for (const x of b) bin += String.fromCharCode(x);
      out.description = btoa(bin);
    }
    return out;
  }

  function connect() {
    return new Promise((resolve, reject) => {
      ws = new WebSocket(wsUrl);
      ws.binaryType = 'arraybuffer';
      const timeout = setTimeout(() => { ws.close(); reject(new Error('Timeout ao conectar.')); }, 10_000);

      ws.addEventListener('open', () => { clearTimeout(timeout); resolve(); });
      ws.addEventListener('message', e => {
        if (typeof e.data !== 'string') return;
        const msg = JSON.parse(e.data);
        if (msg.type === 'slot') mySlot = msg.slot;
        else if (msg.type === 'state') viewers = msg.viewers;
        else if (msg.type === 'need-keyframe') wantKeyframe = true;
        else if (msg.type === 'error') {
          if (running) stop(msg.message);
          else { clearTimeout(timeout); reject(new Error(msg.message)); }
        }
      });
      ws.addEventListener('error', () => { clearTimeout(timeout); reject(new Error('Falha ao conectar.')); });
      ws.addEventListener('close', () => { clearTimeout(timeout); if (running) stop('Conexão caiu.'); });
    });
  }

  async function changeScreen() {
    const fresh = await navigator.mediaDevices.getDisplayMedia(opcoesTela({ fps, comSom: audio }));
    const previous = stream, previousReader = reader;
    stream = fresh;
    const track = fresh.getVideoTracks()[0];
    displaySurface = track.getSettings?.().displaySurface ?? null;
    await track.applyConstraints?.(captureConstraints(fps)).catch(() => {});
    track.contentHint = screenContentHint(fps);
    track.addEventListener('ended', () => stop('Você parou o compartilhamento.'));

    reader = null;
    await previousReader?.cancel().catch(() => {});
    previous?.getTracks().forEach(t => t.stop());
    srcW = srcH = 0; wantKeyframe = true;

    if (video) { video.srcObject = fresh; video.play().catch(() => {}); }
    else pumpDirect(track);

    await audioReader?.cancel().catch(() => {});
    audioReader = null;
    if (audioEncoder?.state === 'configured') { try { audioEncoder.close(); } catch {} }
    audioEncoder = null;
    const novoAudio = prepararSom(track, fresh);
    if (novoAudio) pumpAudio(novoAudio);
    return fresh;
  }

  /** Miniatura JPEG para os cards da atividade. Pequena, fora do fluxo de vídeo. */
  function sendThumbnail(bytes) {
    if (running && ws?.readyState === WebSocket.OPEN) {
      ws.send(empacotar(TIPO_THUMB, 0, bytes));
    }
  }

  /**
   * Troca só a fonte do som, sem tocar no vídeo.
   *
   * Permite som mesmo transmitindo tela inteira: o vídeo continua o mesmo e o
   * som passa a vir de uma aba ou janela (fontes isoladas, sem eco do Discord).
   */
  async function trocarSom() {
    const escolha = await navigator.mediaDevices.getDisplayMedia(
      opcoesTela({ fps, comSom: true, video: true }),
    );

    const faixa = escolha.getAudioTracks()[0];
    const superficie = escolha.getVideoTracks()[0]?.getSettings?.().displaySurface;
    escolha.getVideoTracks().forEach(t => t.stop());

    if (!faixa) {
      escolha.getTracks().forEach(t => t.stop());
      throw new Error('Essa escolha veio sem som. Escolha uma aba e marque "Compartilhar o áudio da guia".');
    }

    const isolado = superficie === 'browser' ||
      (superficie === 'window' && navigator.mediaDevices.getSupportedConstraints?.().restrictOwnAudio);
    if (!isolado) {
      faixa.stop();
      throw new Error('Essa fonte traria o Discord junto (eco). Escolha uma aba ou a janela do aplicativo.');
    }

    await audioReader?.cancel().catch(() => {});
    audioReader = null;
    if (audioEncoder?.state === 'configured') { try { audioEncoder.close(); } catch {} }
    audioEncoder = null;

    faixa.addEventListener('ended', () => onAviso?.('A fonte do som foi fechada.'));
    pumpAudio(faixa);
    return faixa;
  }

  function setQuality({ bitrate: nb, fps: nf } = {}) {
    if (nb) bitrate = nb;
    if (nf) fps = nf;
    if (encoder?.state !== 'configured') return;
    config = { ...config, bitrate, framerate: fps, codec: codecForSize(config.codec, config.width, config.height, fps) };
    outputLimitIndex = 0; networkPressureWindows = 0; srcW = srcH = 0;
    encoder.configure(config);
    wantKeyframe = true;
    frameClock?.postMessage({ type: 'fps', fps });
    stream?.getVideoTracks()[0]?.applyConstraints(captureConstraints(fps)).catch(() => {});
  }

  function cleanup() {
    frameClock?.postMessage({ type: 'stop' }); frameClock?.terminate(); frameClock = null;
    stream?.getTracks().forEach(t => t.stop()); stream = null;
    video?.remove(); video = null; stage = null; stageCtx = null;
  }

  function stop(reason) {
    const wasRunning = running;
    running = false;
    clearInterval(statsTimer); statsTimer = null;
    reader?.cancel().catch(() => {}); reader = null;
    audioReader?.cancel().catch(() => {}); audioReader = null;
    for (const e of [encoder, audioEncoder]) { if (e?.state === 'configured') { try { e.close(); } catch {} } }
    encoder = null; audioEncoder = null;
    if (ws?.readyState === WebSocket.OPEN) { ws.send(JSON.stringify({ type: 'stop' })); ws.close(); }
    ws = null;
    cleanup();
    if (wasRunning) onEnd?.(reason ?? '');
  }

  return {
    start, stop, changeScreen, setQuality, trocarSom, sendThumbnail,
    getSettings: () => ({ bitrate, fps }),
    temSom: () => Boolean(audioEncoder),
    isRunning: () => running,
  };
}
