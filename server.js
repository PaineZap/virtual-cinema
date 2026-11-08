// VIRTUAL CINEMA server — one shared room, host-controlled synced movie
const express = require('express'), http = require('http'), { Server } = require('socket.io');
const app = express(), srv = http.createServer(app);
const io = new Server(srv, { maxHttpBufferSize: 1e5 });
app.use(express.static(__dirname));

const players = new Map();
let hostId = null;
const movie = { type: null, src: null, state: 'stopped', time: 0, at: Date.now() };
const cur = () => movie.state === 'playing' ? movie.time + (Date.now() - movie.at) / 1000 : movie.time;
const pub = () => ({ type: movie.type, src: movie.src, state: movie.state, time: cur() });
const clean = (s, n) => String(s || '').replace(/[<>]/g, '').trim().slice(0, n);
const EMOJI = ['😀','😂','😎','❤️','🔥','👏','👍','👎','😭','🥳','🎬','🍿','🍺','🎉','👀','💀'];
const num = (v, d = 0) => (Number.isFinite(+v) ? +v : d);

io.on('connection', (s) => {
  let lastChat = 0;
  s.on('join', (d) => {
    d = d || {};
    const p = {
      id: s.id, name: clean(d.name, 16) || 'Guest',
      color: /^#[0-9a-f]{6}$/i.test(d.color) ? d.color : '#ffd27a',
      char: d.char === 1 ? 1 : 0, x: num(d.x), y: 0, z: num(d.z, 12), ry: 0, a: 0
    };
    const had = players.has(s.id);
    players.set(s.id, p);
    if (!hostId || !players.has(hostId)) hostId = s.id;
    s.emit('init', { id: s.id, host: hostId, players: [...players.values()], movie: pub() });
    if (!had) s.broadcast.emit('joined', p);
    io.emit('host', hostId);
  });
  s.on('move', (d) => {
    const p = players.get(s.id); if (!p || !d) return;
    p.x = num(d.x); p.y = num(d.y); p.z = num(d.z); p.ry = num(d.ry); p.a = num(d.a) | 0;
  });
  s.on('chat', (t) => {
    const p = players.get(s.id), now = Date.now();
    if (!p || now - lastChat < 700) return;
    t = clean(t, 200); if (!t) return;
    lastChat = now;
    io.emit('chat', { id: s.id, name: p.name, color: p.color, text: t, t: now });
  });
  s.on('emoji', (e) => {
    const p = players.get(s.id), now = Date.now();
    if (!p || !EMOJI.includes(e) || now - lastChat < 400) return;
    lastChat = now;
    io.emit('emoji', { id: s.id, name: p.name, color: p.color, e, t: now });
  });
  s.on('movie', (d) => {
    if (s.id !== hostId || !d) return;
    switch (d.act) {
      case 'load': {
        const src = String(d.src || '').slice(0, 600);
        const ok = d.type === 'yt' ? /^[\w-]{11}$/.test(src) : d.type === 'mp4' && /^https?:\/\//i.test(src);
        if (!ok) return s.emit('err', 'Link video không hợp lệ');
        Object.assign(movie, { type: d.type, src, state: 'paused', time: 0, at: Date.now() });
        break;
      }
      case 'play': if (!movie.src) return; movie.time = cur(); movie.at = Date.now(); movie.state = 'playing'; break;
      case 'pause': if (!movie.src) return; movie.time = cur(); movie.state = 'paused'; break;
      case 'seek': if (!movie.src) return; movie.time = Math.max(0, num(d.t)); movie.at = Date.now(); break;
      case 'stop': Object.assign(movie, { type: null, src: null, state: 'stopped', time: 0, at: Date.now() }); break;
      default: return;
    }
    io.emit('movie', pub());
  });
  s.on('disconnect', () => {
    if (!players.delete(s.id)) return;
    io.emit('left', s.id);
    if (hostId === s.id) { hostId = players.keys().next().value || null; io.emit('host', hostId); }
  });
});

// batched position broadcast, ~15 Hz
setInterval(() => {
  if (players.size < 2) return;
  io.volatile.emit('state', [...players.values()].map((p) => [p.id, p.x, p.y, p.z, p.ry, p.a]));
}, 66);

const PORT = process.env.PORT || 3000;
srv.listen(PORT, () => console.log('VIRTUAL CINEMA chạy tại http://localhost:' + PORT));
