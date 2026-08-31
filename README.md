# ShareScreen Activity

Projeto de estudo: **Discord Embedded Activity** para compartilhamento de tela em tempo real usando WebCodecs API, Socket.io e WebSocket binario.

## Arquitetura Tecnica

### Protocolo Binario Customizado

Cada frame transmitido segue o formato:

```
[1 byte slot][1 byte tipo][8 bytes timestamp][8 bytes relogio][payload]
```

Tipos de frame:
- `0x01` - **Keyframe** (quadro completo, ponto de recuperacao)
- `0x02` - **Delta** (quadro diferencial, depende do keyframe anterior)
- `0x03` - **Audio** (pacote Opus codificado)
- `0x04` - **Thumbnail** (miniatura JPEG para preview nos cards)

### Pipeline de Streaming

```
Captura (getDisplayMedia/getUserMedia)
    |
    v
VideoEncoder (WebCodecs) --> Protocolo binario --> WebSocket
    |                                                  |
    v                                                  v
AudioEncoder (Opus)      --> Protocolo binario --> Servidor Node.js
                                                       |
                                                       v
                                                  Socket.io relay --> VideoDecoder (viewers)
```

### Backpressure por Viewer

O servidor implementa controle de backpressure individual por viewer para evitar acumulo infinito de buffer:

- **Soft limit** (1.5 MB) - Deltas sao descartados e o viewer e marcado como precisando de keyframe
- **Hard limit** (4 MB) - Ate keyframes sao descartados
- Pedidos de keyframe sao limitados a 1 por segundo por streamer

### Qualidade Adaptativa

O broadcaster ajusta automaticamente:
- **Resolucao** - Reduz quando o encoder esta saturado (> 20% de frames dropados)
- **Bitrate** - Reduz quando a rede esta congestionada (backpressure do WebSocket)
- Limites: 1920x1080 -> 1600x900 -> 1280x720

### Stack

| Componente | Tecnologia |
|---|---|
| Frontend (Activity) | Vite + Discord Embedded App SDK |
| Broadcaster | WebCodecs API (VideoEncoder/AudioEncoder) |
| Viewer | WebCodecs API (VideoDecoder/AudioDecoder) |
| Transporte (streamer) | WebSocket binario nativo |
| Transporte (viewer) | Socket.io |
| Servidor | Node.js + Express + ws |
| Audio | Opus via AudioEncoder/AudioDecoder |
| TLS | Opcional via env (cert/key PEM) |

## Requisitos

- Node.js >= 20
- Navegador com WebCodecs (Chrome, Edge, Brave, Firefox recente)
- Aplicacao Discord registrada com Embedded Activities habilitado

## Instalacao

```bash
# Clonar o repositorio
git clone https://github.com/EkaL7/sharescreen-activity.git
cd sharescreen-activity

# Instalar dependencias
npm install

# Configurar variaveis de ambiente
cp .env.example .env
# Editar .env com suas credenciais do Discord

# Desenvolvimento
npm run dev

# Producao
npm run deploy
```

## Configuracao do Discord

1. Crie uma aplicacao em [discord.com/developers](https://discord.com/developers/applications)
2. Habilite **Embedded Activities** nas configuracoes da aplicacao
3. Copie o **Client ID** e **Client Secret** para o `.env`
4. Configure o URL Mapping para apontar para seu servidor

## Estrutura

```
sharescreen-activity/
  server.js              # Servidor Express + Socket.io + WebSocket relay
  src/
    main.js              # Cliente da Activity (viewer)
    style.css            # Estilos da interface
  public/
    share.html           # Pagina do broadcaster (transmissor)
    shared/
      broadcaster.js     # Pipeline de captura + encoding + envio
      frame-worker.js    # Worker para timing preciso de frames
  index.html             # Entry point do Vite
  vite.config.js         # Configuracao do Vite
  ecosystem.config.cjs   # Configuracao PM2
  nginx-share.conf       # Exemplo de config Nginx com TLS
```

## Licenca

Projeto de estudo. Uso educacional.
