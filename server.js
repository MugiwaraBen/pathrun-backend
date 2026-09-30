'use strict';
// Serveur TheRun : sert le jeu (index.html + css/ + js/ + assets/), relaie les positions des joueurs
// par WebSocket et tient le classement des meilleurs temps (multijoueur).
// Aucune dependance : il suffit de Node 16+ ->  node server.js   puis  http://localhost:3000
//
// Le serveur ne fait que : relayer position / animation / pseudo, et enregistrer les temps finaux.
// Les slimes, orbes, checkpoints et morts ne passent jamais par ici : chaque joueur garde sa propre partie.
// Classement : un seul temps (le meilleur) par joueur, identifie par un "uid" aleatoire genere par le client.
// Sauvegarde dans scores.json (variable SCORES_FILE pour changer le chemin ; sur un hebergeur a disque
// ephemere, monter un disque persistant dessus, sinon le classement est perdu a chaque redeploiement).

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const MAX_CLIENTS = 100;
const MAX_MSG_BYTES = 4096;
const MAX_STATES_PER_SEC = 40;
const SKIN_COUNT = 2; // 0 = perso 1 (idle.png...), 1 = perso 2 (idle2.png...) : garder identique au client
const SCORES_FILE = process.env.SCORES_FILE || path.join(ROOT, 'scores.json');
const MAX_SCORES = 500;      // nombre de joueurs gardes en memoire / sur disque
const LB_SEND = 50;          // nombre d'entrees envoyees aux clients
const MAX_LB_MSGS = 8;       // requetes classement par fenetre de 10 s et par connexion

const MIME = {
  '.html': 'text/html; charset=utf-8', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
  '.gif': 'image/gif', '.webp': 'image/webp', '.svg': 'image/svg+xml', '.json': 'application/json',
  '.css': 'text/css; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8', '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.wav': 'audio/wav', '.ico': 'image/x-icon'
};

// ---------------------------------------------------------------- Classement
let scores = new Map(); // uid -> { nick, skin, time, real, deaths, slimes, hearts, tot, date }
try {
  const arr = JSON.parse(fs.readFileSync(SCORES_FILE, 'utf8'));
  if (Array.isArray(arr)) for (const e of arr) if (e && typeof e.uid === 'string' && Number.isFinite(e.time)) { const { uid, ...rest } = e; scores.set(uid, rest); }
  console.log('Classement charge : ' + scores.size + ' entrees');
} catch (e) { /* premier demarrage */ }

let saveTimer = null;
function saveScores() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    const out = JSON.stringify(Array.from(scores, ([uid, e]) => Object.assign({ uid }, e)));
    const tmp = SCORES_FILE + '.tmp';
    fs.writeFile(tmp, out, err => { if (err) return console.error('Sauvegarde classement : ' + err.message); fs.rename(tmp, SCORES_FILE, () => {}); });
  }, 1500);
}
const sortedScores = () => Array.from(scores, ([uid, e]) => Object.assign({ uid }, e)).sort((a, b) => a.time - b.time || a.date - b.date);
function lbPayload(uid, improved) {
  const all = sortedScores();
  const list = all.slice(0, LB_SEND).map(e => ({ nick: e.nick, skin: e.skin, time: e.time, real: e.real, deaths: e.deaths, slimes: e.slimes, hearts: e.hearts, tot: e.tot, date: e.date, you: e.uid === uid }));
  const i = uid ? all.findIndex(e => e.uid === uid) : -1;
  const you = i >= 0 ? { rank: i + 1, time: all[i].time, improved: !!improved } : null;
  return { t: 'lb', list, you, total: all.length };
}
const cleanUid = v => (typeof v === 'string' && /^[A-Za-z0-9_-]{8,64}$/.test(v)) ? v : null;
const cs = (v, max) => { v = Number(v); return Number.isFinite(v) && v >= 0 && v <= max ? v : null; };
function submitScore(m) {
  const uid = cleanUid(m.uid);
  const time = cs(m.time, 86400), real = cs(m.real, 86400);
  const deaths = cs(m.deaths, 100000), slimes = cs(m.slimes, 100000), hearts = cs(m.hearts, 1000), tot = cs(m.tot, 1000);
  if (!uid || time === null || real === null || deaths === null || slimes === null || hearts === null || tot === null || time < 5 || hearts > tot) return { uid, improved: false };
  const nick = cleanNick(m.nick), skin = cleanSkin(m.skin), old = scores.get(uid);
  let improved = false;
  if (!old || time < old.time) {
    scores.set(uid, { nick, skin, time: Math.round(time * 100) / 100, real: Math.round(real * 100) / 100, deaths: Math.round(deaths), slimes: Math.round(slimes), hearts: Math.round(hearts), tot: Math.round(tot), date: Date.now() });
    improved = true;
  } else { old.nick = nick; old.skin = skin; } // pseudo / perso mis a jour, le temps reste le meilleur
  if (scores.size > MAX_SCORES) { const all = sortedScores(); for (const e of all.slice(MAX_SCORES)) scores.delete(e.uid); }
  saveScores();
  return { uid, improved };
}

// ---------------------------------------------------------------- HTTP (fichiers du jeu)
const server = http.createServer((req, res) => {
  let p;
  try { p = decodeURIComponent(new URL(req.url, 'http://x').pathname); }
  catch (e) { res.writeHead(400); return res.end('Bad request'); }
  if (p === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (p === '/api/leaderboard') { // lecture seule (CORS ouvert)
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-cache' });
    return res.end(JSON.stringify(lbPayload(null)));
  }
  if (p === '/') p = '/index.html';
  // seuls index.html et les dossiers css/ js/ assets/ sont publics (pas server.js, pas scores.json, pas de fichiers caches)
  if (p !== '/index.html' && !/^\/(assets|css|js)\//.test(p)) { res.writeHead(404); return res.end('Not found'); }
  if (p.split('/').some(seg => seg.startsWith('.'))) { res.writeHead(404); return res.end('Not found'); }
  const file = path.join(ROOT, p);
  if (!file.startsWith(ROOT + path.sep)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file).toLowerCase()] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

// ---------------------------------------------------------------- WebSocket (implementation minimale, RFC 6455)
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const clients = new Map();
let nextId = 1;

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.end('HTTP/1.1 400 Bad Request\r\n\r\n'); return; }
  if (clients.size >= MAX_CLIENTS) { socket.end('HTTP/1.1 503 Service Unavailable\r\n\r\n'); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  addClient(socket);
});

function addClient(socket) {
  const c = { id: nextId++, socket, buf: Buffer.alloc(0), nick: null, skin: 0, state: null, alive: true, dead: false, t0: Date.now(), win: 0, n: 0, lbWin: 0, lbN: 0 };
  clients.set(c.id, c);
  socket.on('data', chunk => {
    c.alive = true;
    c.buf = Buffer.concat([c.buf, chunk]);
    if (c.buf.length > 4 * MAX_MSG_BYTES) return drop(c);
    parse(c);
  });
  socket.on('close', () => drop(c));
  socket.on('error', () => drop(c));
}

function parse(c) {
  for (;;) {
    if (c.dead) return;
    const b = c.buf;
    if (b.length < 2) return;
    const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; if (b.readUInt32BE(2) !== 0) return drop(c); len = b.readUInt32BE(6); off = 10; }
    if (!masked || len > MAX_MSG_BYTES) return drop(c); // un client doit masquer ses trames
    if (b.length < off + 4 + len) return;               // trame incomplete : on attend la suite
    const mask = b.subarray(off, off + 4), payload = Buffer.alloc(len);
    for (let i = 0; i < len; i++) payload[i] = b[off + 4 + i] ^ mask[i & 3];
    c.buf = b.subarray(off + 4 + len);
    if (op === 8) { sendFrame(c, 8, Buffer.alloc(0)); return drop(c); }
    if (op === 9) { sendFrame(c, 10, payload); continue; }
    if (op === 10) continue;
    if (!fin || op !== 1) return drop(c);               // ni fragmentation ni binaire
    onMessage(c, payload.toString('utf8'));
  }
}

function sendFrame(c, op, payload) {
  const s = c.socket;
  if (c.dead || s.destroyed || !s.writable) return;
  if (s.writableLength > 262144) return drop(c);        // client trop lent : on le lache
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
  s.write(Buffer.concat([head, payload]));
}
const send = (c, obj) => sendFrame(c, 1, Buffer.from(JSON.stringify(obj)));
function broadcast(from, obj) {
  const data = Buffer.from(JSON.stringify(obj));
  for (const o of clients.values()) if (o !== from && o.nick && !o.dead) sendFrame(o, 1, data);
}

function drop(c) {
  if (c.dead) return;
  c.dead = true;
  clients.delete(c.id);
  try { c.socket.destroy(); } catch (e) { /* deja ferme */ }
  if (c.nick) { broadcast(c, { t: 'leave', id: c.id }); console.log('- ' + c.nick + ' (' + countJoined() + ' en ligne)'); }
}
const countJoined = () => { let n = 0; for (const o of clients.values()) if (o.nick) n++; return n; };

// ---------------------------------------------------------------- Messages
const STATES = new Set(['idle', 'run', 'jumpRise', 'jumpFall', 'wallClimb', 'wallSlide', 'dash']);
function num(v, lim) { v = Number(v); return Number.isFinite(v) && Math.abs(v) < lim ? Math.round(v) : null; }
function cleanNick(v) {
  const s = Array.from(String(v == null ? '' : v).replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim()).slice(0, 16).join('').trim();
  return s || 'Joueur';
}
function cleanSkin(v) {
  const n = Number(v);
  return Number.isInteger(n) && n >= 0 && n < SKIN_COUNT ? n : 0;
}
function uniqueNick(base, self) {
  const taken = new Set();
  for (const o of clients.values()) if (o !== self && o.nick) taken.add(o.nick.toLowerCase());
  let nick = base, i = 2;
  while (taken.has(nick.toLowerCase())) nick = Array.from(base).slice(0, 13).join('') + ' ' + i++;
  return nick;
}
function cleanState(m) {
  const x = num(m.x, 1e6), y = num(m.y, 1e6);
  if (x === null || y === null) return null;
  return { x, y, vx: num(m.vx, 1e5) || 0, vy: num(m.vy, 1e5) || 0, f: m.f < 0 ? -1 : 1, st: STATES.has(m.st) ? m.st : 'idle', d: m.d ? 1 : 0 };
}

function onMessage(c, text) {
  let m;
  try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
  if (m.t === 'lb' || m.t === 'score') {          // classement : possible sans avoir rejoint la partie
    const now = Date.now();
    if (now - c.lbWin >= 10000) { c.lbWin = now; c.lbN = 0; }
    if (++c.lbN > MAX_LB_MSGS) return;
    let uid = cleanUid(m.uid), improved = false;
    if (m.t === 'score') { const r = submitScore(m); uid = r.uid; improved = r.improved; }
    send(c, lbPayload(uid, improved));
    return;
  }
  if (m.t === 'join') {
    if (c.nick) return;
    c.nick = uniqueNick(cleanNick(m.nick), c);
    c.skin = cleanSkin(m.skin);
    const peers = [];
    for (const o of clients.values()) if (o !== c && o.nick) peers.push(Object.assign({ id: o.id, nick: o.nick, skin: o.skin }, o.state || {}));
    send(c, { t: 'welcome', id: c.id, peers });
    broadcast(c, { t: 'join', id: c.id, nick: c.nick, skin: c.skin });
    console.log('+ ' + c.nick + ' (' + countJoined() + ' en ligne)');
  } else if (m.t === 's' && c.nick) {
    const now = Date.now();
    if (now - c.win >= 1000) { c.win = now; c.n = 0; }
    if (++c.n > MAX_STATES_PER_SEC) return;
    const st = cleanState(m);
    if (!st) return;
    if (m.skin !== undefined) c.skin = cleanSkin(m.skin); // le perso peut aussi etre mis a jour en cours de partie
    c.state = st;
    broadcast(c, Object.assign({ t: 's', id: c.id, skin: c.skin }, st));
  }
}

// Battement de coeur : ferme les connexions mortes et les sockets qui n'envoient jamais de pseudo
setInterval(() => {
  const now = Date.now();
  for (const c of [...clients.values()]) {
    if (!c.alive || (!c.nick && now - c.t0 > 15000)) { drop(c); continue; }
    c.alive = false;
    sendFrame(c, 9, Buffer.alloc(0));
  }
}, 15000);

server.listen(PORT, () => console.log('TheRun : http://localhost:' + PORT));
