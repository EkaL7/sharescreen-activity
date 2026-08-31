module.exports = {
  apps: [{
    name: 'sharescreen',
    script: 'server.js',
    cwd: '/opt/sharescreen-bot',
    env: { NODE_ENV: 'production' },
    exec_mode: 'fork',
    instances: 1,
    autorestart: true,
    max_memory_restart: '256M',
  }],
};
