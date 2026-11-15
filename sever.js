// Dino Arena online server: a tiny relay. It keeps rooms, shares each player's
// presence (position, health...) and forwards events (hits, kills).
// Run:  npm install && npm start      (listens on $PORT or 8080)
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PER_ROOM = 12;          // players per game server (the lobby is unlimited)
const MAX_ROOMS_PER_CLIENT = 3;   // lobby + game server + spare
const NAME_OK = /^[a-z0-9][a-z0-9_.-]{0,47}$/;

const server = http.createServer((req, res) => { res.writeHead(200); res.end('Dino Arena server is running'); });
const wss = new WebSocketServer({ server, maxPayload: 8192 });
const rooms = new Map(); // name -> Map(id -> { ws, p })

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const getRoom = (n) => { if (!rooms.has(n)) rooms.set(n, new Map()); return rooms.get(n); };
function bcast(R, from, o) { for (const [id, e] of R) if (id !== from) send(e.ws, o); }
function drop(ws, name) {
  const R = rooms.get(name); if (!R || !R.has(ws.id)) return;
  R.delete(ws.id); ws.rooms.delete(name);
  bcast(R, ws.id, { t: 'left', room: name, peer: ws.id });
  if (!R.size) rooms.delete(name);
}

wss.on('connection', (ws) => {
  ws.id = Math.random().toString(36).slice(2, 10);
  ws.rooms = new Set(); ws.count = 0;
  send(ws, { t: 'hello', id: ws.id });
  ws.on('message', (buf) => {
    if (++ws.count > 150) return;                 // at most ~150 messages per second
    let m; try { m = JSON.parse(buf); } catch (e) { return; }
    if (!m || typeof m.room !== 'string' || !NAME_OK.test(m.room)) return;
    const R = getRoom(m.room);
    if (m.t === 'join') {
      if (!R.has(ws.id)) {
        if (ws.rooms.size >= MAX_ROOMS_PER_CLIENT) return;
        if (m.room !== 'lobby' && R.size >= MAX_PER_ROOM) { send(ws, { t: 'full', room: m.room }); return; }
        R.set(ws.id, { ws, p: {} }); ws.rooms.add(m.room);
      }
      send(ws, { t: 'peers', room: m.room, list: [...R].map(([id, e]) => ({ peer: id, p: e.p })) });
      return;
    }
    const me = R.get(ws.id); if (!me) return;
    if (m.t === 'pres' && m.p && typeof m.p === 'object') {
      const next = Object.assign({}, me.p, m.p);
      for (const k in m.p) if (m.p[k] === null) delete next[k];
      if (JSON.stringify(next).length > 2000) return;
      me.p = next;
      bcast(R, ws.id, { t: 'pres', room: m.room, peer: ws.id, p: me.p });
    } else if (m.t === 'ev' && typeof m.topic === 'string' && m.topic.length < 32) {
      bcast(R, ws.id, { t: 'ev', room: m.room, peer: ws.id, topic: m.topic, data: m.data });
    } else if (m.t === 'leave') drop(ws, m.room);
  });
  ws.on('close', () => { for (const n of [...ws.rooms]) drop(ws, n); });
});
setInterval(() => { for (const ws of wss.clients) ws.count = 0; }, 1000);
server.listen(PORT, () => console.log('Dino Arena server listening on port ' + PORT));
