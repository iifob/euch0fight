// Euch Fight game server: serves the game and runs online rooms + the developer sound storage.
// No outside packages needed. Start with: node server.js
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const DEFAULT_PASS = process.env.DEV_PASSWORD || 'Euch2026';
const PUBLIC = __dirname;
const FILES = new Set(['/index.html', '/logo.webp']);
fs.mkdirSync(DATA_DIR, { recursive: true });

// ---------- tiny document store (developer sounds + settings) ----------
const sha = t => crypto.createHash('sha256').update(String(t)).digest('hex');
const okPath = p => typeof p === 'string' && /^(config|audio)\/[A-Za-z0-9_\-]{1,60}$/.test(p);
const fileOf = p => path.join(DATA_DIR, p.replace('/', '__') + '.json');
function dbGet(p) { try { return JSON.parse(fs.readFileSync(fileOf(p), 'utf8')); } catch (e) { return null; } }
function dbSet(p, d) { fs.writeFileSync(fileOf(p), JSON.stringify(d)); }
function dbDel(p) { try { fs.unlinkSync(fileOf(p)); } catch (e) {} }
function pub(p, d) { if (p === 'config/sounds' && d) { const x = Object.assign({}, d); delete x.passHash; return x; } return d; }
function passHash() { const c = dbGet('config/sounds'); return (c && c.passHash) || sha(DEFAULT_PASS); }

// ---------- static files ----------
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.webp': 'image/webp', '.ico': 'image/x-icon', '.json': 'application/json' };
const server = http.createServer((req, res) => {
  let u = decodeURIComponent((req.url || '/').split('?')[0]);
  if (u === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (u === '/') u = '/index.html';
  if (!FILES.has(u)) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('not found'); }
  const f = path.normalize(path.join(PUBLIC, u));
  if (!f.startsWith(PUBLIC)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (err, buf) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(buf);
  });
});

// ---------- minimal WebSocket (RFC 6455) ----------
const MAX_MSG = 4 * 1024 * 1024;
function wsSend(c, obj) {
  if (c.closed) return;
  const data = Buffer.from(typeof obj === 'string' ? obj : JSON.stringify(obj));
  const n = data.length;
  let head;
  if (n < 126) { head = Buffer.alloc(2); head[1] = n; }
  else if (n < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(n, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(n), 2); }
  head[0] = 0x81;
  try { c.sock.write(Buffer.concat([head, data])); } catch (e) {}
}
function wsClose(c) { if (c.closed) return; c.closed = true; try { c.sock.write(Buffer.from([0x88, 0])); c.sock.end(); } catch (e) {} onClose(c); }
server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.url || '').split('?')[0] !== '/ws') { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = { sock, id: 'p' + crypto.randomBytes(5).toString('hex'), room: null, presence: {}, subs: new Set(), buf: Buffer.alloc(0), frag: [], closed: false, alive: true };
  sock.on('data', d => {
    c.buf = Buffer.concat([c.buf, d]);
    while (c.buf.length >= 2) {
      const b0 = c.buf[0], b1 = c.buf[1];
      let len = b1 & 127, off = 2;
      if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_MSG) { wsClose(c); return; }
      const masked = b1 & 128;
      if (c.buf.length < off + (masked ? 4 : 0) + len) return;
      let payload;
      if (masked) { const m = c.buf.slice(off, off + 4); payload = Buffer.from(c.buf.slice(off + 4, off + 4 + len)); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; off += 4; }
      else payload = c.buf.slice(off, off + len);
      c.buf = c.buf.slice(off + len);
      const op = b0 & 15, fin = b0 & 128;
      if (op === 8) { wsClose(c); return; }
      if (op === 9) { try { sock.write(Buffer.concat([Buffer.from([0x8a, payload.length]), payload])); } catch (e) {} continue; }
      if (op === 10) { c.alive = true; continue; }
      if (op === 1 || op === 2 || op === 0) {
        c.frag.push(payload);
        if (fin) { const msg = Buffer.concat(c.frag).toString('utf8'); c.frag = []; c.alive = true; try { onMessage(c, JSON.parse(msg)); } catch (e) {} }
      }
    }
  });
  sock.on('close', () => { c.closed = true; onClose(c); });
  sock.on('error', () => { c.closed = true; onClose(c); });
  wsSend(c, { t: 'hello', id: c.id });
});
// drop dead connections
setInterval(() => { for (const r of rooms.values()) for (const c of r) { if (!c.alive) { wsClose(c); continue; } c.alive = false; try { c.sock.write(Buffer.from([0x89, 0])); } catch (e) {} } }, 20000);

// ---------- rooms ----------
const rooms = new Map();
const subs = new Map(); // db path -> Set of connections
const peerOf = c => ({ peer: c.id, presence: c.presence });
function sendPeers(room, joined, left) {
  const r = rooms.get(room); if (!r) return;
  const peers = [...r].map(peerOf);
  for (const c of r) wsSend(c, { t: 'peers', peers, joined, left });
}
function leaveRoom(c) {
  if (!c.room) return; const room = c.room; const r = rooms.get(room); c.room = null;
  if (!r) return; r.delete(c);
  if (!r.size) rooms.delete(room); else sendPeers(room, [], [peerOf(c)]);
}
function onClose(c) {
  if (c.gone) return; c.gone = true; leaveRoom(c);
  for (const p of c.subs) { const s = subs.get(p); if (s) { s.delete(c); if (!s.size) subs.delete(p); } }
}
function notify(p) { const s = subs.get(p); if (!s) return; const data = dbGet(p); for (const c of s) wsSend(c, { t: 'snap', path: p, data: pub(p, data) }); }
function onMessage(c, m) {
  if (!m || typeof m.t !== 'string') return;
  if (m.t === 'join') {
    const room = String(m.room || '').slice(0, 40); if (!room) return;
    leaveRoom(c); c.room = room; c.presence = {};
    if (!rooms.has(room)) rooms.set(room, new Set());
    const r = rooms.get(room);
    if (r.size >= 8) { c.room = null; wsSend(c, { t: 'full' }); return; }
    r.add(c); wsSend(c, { t: 'joined', room, id: c.id }); sendPeers(room, [peerOf(c)], []);
  } else if (m.t === 'leave') leaveRoom(c);
  else if (m.t === 'pres') {
    if (!c.room || !m.p || typeof m.p !== 'object') return;
    c.presence = m.p;
    const r = rooms.get(c.room); if (!r) return;
    const msg = JSON.stringify({ t: 'pres', peer: c.id, p: m.p });
    for (const o of r) if (o !== c) wsSend(o, msg);
  } else if (m.t === 'get') {
    if (!okPath(m.path)) return wsSend(c, { t: 'res', id: m.id, err: 'bad path' });
    wsSend(c, { t: 'res', id: m.id, data: pub(m.path, dbGet(m.path)) });
  } else if (m.t === 'sub') {
    if (!okPath(m.path)) return;
    if (!subs.has(m.path)) subs.set(m.path, new Set());
    subs.get(m.path).add(c); c.subs.add(m.path); wsSend(c, { t: 'snap', path: m.path, data: pub(m.path, dbGet(m.path)) });
  } else if (m.t === 'set' || m.t === 'del') {
    if (!okPath(m.path)) return wsSend(c, { t: 'res', id: m.id, err: 'bad path' });
    if (m.auth !== passHash()) return wsSend(c, { t: 'res', id: m.id, err: 'denied' });
    if (m.t === 'set') { if (!m.data || typeof m.data !== 'object') return wsSend(c, { t: 'res', id: m.id, err: 'bad data' }); const d = Object.assign({}, m.data); if (m.path === 'config/sounds' && !d.passHash) d.passHash = passHash(); dbSet(m.path, d); }
    else dbDel(m.path);
    wsSend(c, { t: 'res', id: m.id, ok: true }); notify(m.path);
  } else if (m.t === 'auth') {
    wsSend(c, { t: 'res', id: m.id, ok: m.auth === passHash() });
  }
}

server.listen(PORT, () => console.log('Euch Fight server running on port ' + PORT));
