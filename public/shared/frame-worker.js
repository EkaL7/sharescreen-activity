let timer = null;

self.onmessage = (e) => {
  const { type, fps } = e.data;
  if (type === 'start' || type === 'fps') {
    clearInterval(timer);
    timer = setInterval(() => self.postMessage('tick'), Math.round(1000 / (fps || 30)));
  } else if (type === 'stop') {
    clearInterval(timer);
    timer = null;
  }
};
