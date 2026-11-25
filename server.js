// ============================================================
//  CLUBE PRO 3D — SERVIDOR ONLINE
//  Contas com ID, amigos, times (clubes), salas personalizadas,
//  fila ranqueada, ranking (top 100) e partidas ao vivo.
//  Não precisa instalar nada além do Node.js:  node server.js
// ============================================================
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const GAME_FILE = path.join(__dirname, 'jogo.html');
const DAY = 86400000;
const NAME_WAIT = 30 * DAY; // nome só pode ser trocado a cada 30 dias
const PAL = ['#d62828', '#1d4ed8', '#15803d', '#facc15', '#f1f5f9', '#18181b', '#7c3aed', '#f97316', '#0ea5e9', '#be185d'];

// ---------- "banco de dados" (arquivo data.json) ----------
let db = { users: {}, tokens: {}, clubs: {} };
try { db = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch (e) { /* primeiro uso */ }
db.users = db.users || {}; db.tokens = db.tokens || {}; db.clubs = db.clubs || {};
let saveTimer = null;
function save() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; fs.writeFile(DATA_FILE, JSON.stringify(db), () => {}); }, 500);
}

// ---------- servidor HTTP (entrega o jogo) ----------
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/jogo.html') {
    fs.readFile(GAME_FILE, (err, data) => {
      if (err) { res.writeHead(500); res.end('jogo.html não encontrado ao lado do server.js'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(data);
    });
  } else if (url === '/saude') { res.writeHead(200); res.end('ok'); }
  else { res.writeHead(404); res.end('não encontrado'); }
});

// ---------- WebSocket simples (sem bibliotecas) ----------
function wsAccept(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return null; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = {
    socket, buf: Buffer.alloc(0), frag: null, alive: true, uid: null, onmessage: null, onclose: null,
    send(str) {
      if (!this.alive) return;
      const data = Buffer.from(str), len = data.length;
      let h;
      if (len < 126) h = Buffer.from([0x81, len]);
      else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x81; h[1] = 126; h.writeUInt16BE(len, 2); }
      else { h = Buffer.alloc(10); h[0] = 0x81; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
      try { socket.write(Buffer.concat([h, data])); } catch (e) { /* ignora */ }
    },
    close() {
      if (!this.alive) return; this.alive = false;
      try { socket.end(Buffer.from([0x88, 0])); } catch (e) { /* ignora */ }
      if (this.onclose) this.onclose();
    },
  };
  socket.on('data', chunk => { c.buf = Buffer.concat([c.buf, chunk]); readFrames(c); });
  socket.on('close', () => { if (c.alive) { c.alive = false; if (c.onclose) c.onclose(); } });
  socket.on('error', () => {});
  return c;
}
function readFrames(c) {
  while (c.alive && c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1], fin = b0 & 0x80, op = b0 & 0x0f, masked = b1 & 0x80;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > 1 << 20) { c.close(); return; }
    const need = off + (masked ? 4 : 0) + len;
    if (c.buf.length < need) return;
    let payload = Buffer.from(c.buf.subarray(off + (masked ? 4 : 0), need));
    if (masked) { const mask = c.buf.subarray(off, off + 4); for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3]; }
    c.buf = c.buf.subarray(need);
    if (op === 8) { c.close(); return; }
    if (op === 9) { if (payload.length < 126) c.socket.write(Buffer.concat([Buffer.from([0x8A, payload.length]), payload])); continue; }
    if (op === 1 || op === 0) {
      c.frag = c.frag ? Buffer.concat([c.frag, payload]) : payload;
      if (fin) { const txt = c.frag.toString('utf8'); c.frag = null; if (c.onmessage) c.onmessage(txt); }
    }
  }
}

// ---------- estado em memória ----------
const conns = new Map();    // id do jogador -> conexão
const lobbyOf = new Map();  // id -> código da sala
const matchOf = new Map();  // id -> id da partida
const lobbies = new Map();  // código -> sala
const matches = new Map();  // id -> partida
let queue = [];             // salas ranqueadas procurando partida

const clean = (s, max) => String(s == null ? '' : s).replace(/[<>"'`\\]/g, '').replace(/\s+/g, ' ').trim().slice(0, max);
function newId() { let id; do { id = String(Math.floor(100000 + Math.random() * 900000)); } while (db.users[id]); return id; }
function newCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; let c;
  do { c = ''; for (let i = 0; i < 5; i++) c += A[Math.floor(Math.random() * A.length)]; } while (lobbies.has(c));
  return c;
}
function send(uid, o) { const c = conns.get(uid); if (c) c.send(JSON.stringify(o)); }
const toast = (uid, msg) => send(uid, { t: 'toast', msg });
const err = (uid, msg) => send(uid, { t: 'error', msg });

function pub(u) {
  const club = u.clubId && db.clubs[u.clubId];
  return { id: u.id, name: u.name, rating: Math.round(u.rating), w: u.w, d: u.d, l: u.l, games: u.games, club: club ? club.name : null };
}
const meMsg = u => ({ t: 'me', me: Object.assign(pub(u), { nextNameChange: u.nameChangedAt + NAME_WAIT, pos: u.pos, num: u.num }) });
function friendsMsg(u) {
  return {
    t: 'friends',
    list: u.friends.map(id => db.users[id]).filter(Boolean).map(f => Object.assign(pub(f), { online: conns.has(f.id), playing: matchOf.has(f.id) })),
    requests: u.reqIn.map(id => db.users[id]).filter(Boolean).map(f => ({ id: f.id, name: f.name })),
    clubInvites: (u.clubInv || []).map(id => db.clubs[id]).filter(Boolean).map(c => ({ id: c.id, name: c.name, color: c.color })),
  };
}
function clubMsg(u) {
  const c = u.clubId && db.clubs[u.clubId];
  return { t: 'club', club: c ? { id: c.id, name: c.name, color: c.color, owner: c.owner, members: c.members.map(id => db.users[id]).filter(Boolean).map(m => Object.assign(pub(m), { online: conns.has(m.id) })) } : null };
}
function lobbyMsg(l) {
  return {
    t: 'lobby', lobby: l ? {
      code: l.code, mode: l.mode, host: l.host, status: l.status, settings: l.settings,
      members: l.members.map(m => { const u = db.users[m.id]; return { id: m.id, name: u ? u.name : '?', side: m.side, pos: m.pos, rating: u ? Math.round(u.rating) : 1000 }; }),
    } : null,
  };
}
function pushAll(uid) {
  const u = db.users[uid]; if (!u || !conns.has(uid)) return;
  send(uid, meMsg(u)); send(uid, friendsMsg(u)); send(uid, clubMsg(u)); send(uid, lobbyMsg(lobbies.get(lobbyOf.get(uid))));
}
function pushFriendsOf(uid) { const u = db.users[uid]; if (u) for (const f of u.friends) if (conns.has(f) && db.users[f]) send(f, friendsMsg(db.users[f])); }
function pushLobby(l) { for (const m of l.members) send(m.id, lobbyMsg(l)); }
function pushClub(cid) { const c = db.clubs[cid]; if (c) for (const id of c.members) if (conns.has(id)) { send(id, clubMsg(db.users[id])); send(id, meMsg(db.users[id])); } }

// ---------- salas ----------
function leaveLobby(uid) {
  const code = lobbyOf.get(uid); if (!code) return;
  lobbyOf.delete(uid);
  const l = lobbies.get(code);
  if (l) {
    l.members = l.members.filter(m => m.id !== uid);
    queue = queue.filter(c => c !== code);
    if (l.status === 'queue') l.status = 'lobby';
    if (!l.members.length) lobbies.delete(code);
    else { if (l.host === uid) l.host = l.members[0].id; pushLobby(l); }
  }
  send(uid, lobbyMsg(null));
}
function joinLobby(uid, l) {
  if (lobbyOf.get(uid) === l.code) return;
  if (matchOf.has(uid)) return err(uid, 'Você está numa partida.');
  if (l.status !== 'lobby') return err(uid, 'Essa sala já está jogando ou procurando partida.');
  const maxPlayers = l.mode === 'ranked' ? 7 : 14;
  if (l.members.length >= maxPlayers) return err(uid, 'A sala está cheia.');
  leaveLobby(uid);
  const u = db.users[uid];
  const side = l.mode === 'ranked' ? 0 : (l.members.filter(m => m.side === 0).length <= l.members.filter(m => m.side === 1).length ? 0 : 1);
  l.members.push({ id: uid, side, pos: u.pos || 'MEI' });
  lobbyOf.set(uid, l.code);
  pushLobby(l);
}
function sideInfo(humans, s, avoidColor, avoidName) {
  const h = humans.find(x => x.side === s), u = h && db.users[h.id];
  let club = u && u.clubId && db.clubs[u.clubId];
  if (club && club.name === avoidName) club = null; // os dois lados são do mesmo clube: usa "Time de <nome>"
  let color = club ? club.color : null;
  if (!color || color === avoidColor) color = PAL.find(c => c !== avoidColor && (s === 0 ? true : c !== '#f1f5f9')) || PAL[1];
  return { name: club ? club.name : (u ? 'Time de ' + u.name : (s ? 'Visitantes' : 'Mandantes')), color };
}
function startMatch(humans, hostId, ranked, settings, lobbyCodes) {
  const id = crypto.randomBytes(6).toString('hex');
  const s0 = sideInfo(humans, 0, null, null), s1 = sideInfo(humans, 1, s0.color, s0.name);
  const m = { id, host: hostId, members: humans.map(h => h.id), humans, ranked, lobbies: lobbyCodes, ended: false };
  matches.set(id, m);
  for (const h of humans) matchOf.set(h.id, id);
  for (const c of lobbyCodes) { const l = lobbies.get(c); if (l) { l.status = 'playing'; pushLobby(l); } }
  const payload = {
    t: 'matchStart', matchId: id, hostId, ranked, settings, sides: [s0, s1],
    humans: humans.map(h => { const u = db.users[h.id]; return { id: h.id, team: h.side, pos: h.pos, name: u.name, num: u.num, look: u.look }; }),
  };
  for (const h of humans) { send(h.id, payload); pushFriendsOf(h.id); }
}
function finishMatch(m) {
  m.ended = true; matches.delete(m.id);
  for (const id of m.members) matchOf.delete(id);
  for (const h of m.humans) matchOf.delete(h.id);
  for (const c of m.lobbies) { const l = lobbies.get(c); if (l) { l.status = 'lobby'; pushLobby(l); } }
  for (const h of m.humans) pushFriendsOf(h.id);
}
function tryMatchmaking() {
  queue = queue.filter(c => lobbies.has(c) && lobbies.get(c).status === 'queue');
  while (queue.length >= 2) {
    const a = lobbies.get(queue.shift()), b = lobbies.get(queue.shift());
    const humans = [...a.members.map(m => ({ id: m.id, side: 0, pos: m.pos })), ...b.members.map(m => ({ id: m.id, side: 1, pos: m.pos }))];
    startMatch(humans, a.host, true, { dur: 300, diff: 'normal' }, [a.code, b.code]);
  }
}
function applyElo(m, sc) {
  const sides = [0, 1].map(s => m.humans.filter(h => h.side === s).map(h => db.users[h.id]).filter(Boolean));
  const changes = {};
  if (!sides[0].length || !sides[1].length) return changes;
  const avg = list => list.reduce((a, u) => a + u.rating, 0) / list.length;
  const e0 = 1 / (1 + Math.pow(10, (avg(sides[1]) - avg(sides[0])) / 400));
  const s0 = sc[0] > sc[1] ? 1 : sc[0] < sc[1] ? 0 : 0.5;
  sides.forEach((list, s) => {
    const S = s === 0 ? s0 : 1 - s0, E = s === 0 ? e0 : 1 - e0;
    const delta = Math.round(32 * (S - E) + (S === 1 ? Math.min(3, Math.abs(sc[0] - sc[1])) : 0));
    for (const u of list) {
      const old = Math.round(u.rating);
      u.rating = Math.max(0, u.rating + delta); u.games++;
      if (S === 1) u.w++; else if (S === 0) u.l++; else u.d++;
      changes[u.id] = { old, now: Math.round(u.rating) };
    }
  });
  save();
  return changes;
}
function leaveMatch(uid) {
  const mid = matchOf.get(uid); if (!mid) return;
  matchOf.delete(uid);
  const m = matches.get(mid); if (!m || m.ended) return;
  m.members = m.members.filter(x => x !== uid);
  if (m.host === uid) {
    for (const id of m.members) send(id, { t: 'matchAbort', msg: 'Quem criou a partida saiu. A partida foi encerrada.' });
    finishMatch(m);
  } else send(m.host, { t: 'playerLeft', id: uid });
}

// ---------- mensagens dos jogadores ----------
function handle(c, msg) {
  const t = msg.t;
  if (t === 'ping') return;
  if (t === 'hello') {
    let u = msg.token && db.tokens[msg.token] && db.users[db.tokens[msg.token]];
    let token = msg.token;
    if (!u) {
      const id = newId();
      token = crypto.randomBytes(16).toString('hex');
      u = { id, name: clean(msg.name, 14) || 'Jogador', nameChangedAt: Date.now(), created: Date.now(), rating: 1000, w: 0, d: 0, l: 0, games: 0, friends: [], reqIn: [], clubId: null, clubInv: [] };
      db.users[id] = u; db.tokens[token] = id;
    }
    if (msg.look && typeof msg.look === 'object') u.look = msg.look;
    u.num = Math.max(1, Math.min(99, parseInt(msg.num) || 10));
    u.pos = ['ATA', 'MEI', 'ZAG', 'GOL'].includes(msg.pos) ? msg.pos : 'MEI';
    const old = conns.get(u.id); if (old && old !== c) { old.uid = null; old.close(); }
    c.uid = u.id; conns.set(u.id, c);
    save();
    c.send(JSON.stringify({ t: 'welcome', token, me: meMsg(u).me }));
    pushAll(u.id); pushFriendsOf(u.id);
    return;
  }
  const uid = c.uid, u = uid && db.users[uid];
  if (!u) return;
  const target = msg.id != null ? db.users[String(msg.id)] : null;
  const myLobby = lobbies.get(lobbyOf.get(uid));

  switch (t) {
    case 'setProfile':
      if (msg.look && typeof msg.look === 'object') u.look = msg.look;
      if (msg.num) u.num = Math.max(1, Math.min(99, parseInt(msg.num) || 10));
      if (['ATA', 'MEI', 'ZAG', 'GOL'].includes(msg.pos)) u.pos = msg.pos;
      save(); break;
    case 'setName': {
      const name = clean(msg.name, 14);
      if (name.length < 2) return err(uid, 'O nome precisa ter pelo menos 2 letras.');
      const wait = u.nameChangedAt + NAME_WAIT - Date.now();
      if (wait > 0) return err(uid, `Você só pode trocar o nome daqui a ${Math.ceil(wait / DAY)} dia(s).`);
      u.name = name; u.nameChangedAt = Date.now(); save();
      send(uid, meMsg(u)); pushFriendsOf(uid); toast(uid, 'Nome trocado! O próximo troca só daqui a 30 dias.');
      break;
    }
    // --- amigos ---
    case 'friendAdd':
      if (!target) return err(uid, 'Não existe jogador com esse ID.');
      if (target.id === uid) return err(uid, 'Esse é o seu próprio ID.');
      if (u.friends.includes(target.id)) return err(uid, 'Vocês já são amigos.');
      if (u.reqIn.includes(target.id)) { // ele já tinha me chamado: vira amizade
        u.reqIn = u.reqIn.filter(x => x !== target.id); u.friends.push(target.id); target.friends.push(uid);
        save(); send(uid, friendsMsg(u)); send(target.id, friendsMsg(target)); toast(target.id, `${u.name} aceitou sua amizade!`);
        return;
      }
      if (!target.reqIn.includes(uid)) target.reqIn.push(uid);
      save(); toast(uid, 'Pedido de amizade enviado para ' + target.name + '.');
      send(target.id, friendsMsg(target)); toast(target.id, `${u.name} (#${uid}) quer ser seu amigo.`);
      break;
    case 'friendAccept':
      if (!target || !u.reqIn.includes(target.id)) return;
      u.reqIn = u.reqIn.filter(x => x !== target.id);
      if (!u.friends.includes(target.id)) u.friends.push(target.id);
      if (!target.friends.includes(uid)) target.friends.push(uid);
      save(); send(uid, friendsMsg(u)); send(target.id, friendsMsg(target)); toast(target.id, `${u.name} aceitou sua amizade!`);
      break;
    case 'friendDecline':
      u.reqIn = u.reqIn.filter(x => x !== String(msg.id)); save(); send(uid, friendsMsg(u)); break;
    case 'friendRemove':
      if (!target) return;
      u.friends = u.friends.filter(x => x !== target.id); target.friends = target.friends.filter(x => x !== uid);
      save(); send(uid, friendsMsg(u)); send(target.id, friendsMsg(target)); break;
    // --- time (clube) ---
    case 'clubCreate': {
      if (u.clubId) return err(uid, 'Você já está num time. Saia dele primeiro.');
      const name = clean(msg.name, 16);
      if (name.length < 3) return err(uid, 'O nome do time precisa ter pelo menos 3 letras.');
      const id = 'c' + crypto.randomBytes(4).toString('hex');
      db.clubs[id] = { id, name, color: PAL.includes(msg.color) ? msg.color : PAL[0], owner: uid, members: [uid], created: Date.now() };
      u.clubId = id; save(); pushClub(id); toast(uid, 'Time criado! Agora chame seus amigos.');
      break;
    }
    case 'clubInvite': {
      const club = u.clubId && db.clubs[u.clubId];
      if (!club) return err(uid, 'Você não tem time.');
      if (!target) return err(uid, 'Jogador não encontrado.');
      if (club.members.includes(target.id)) return err(uid, 'Ele já está no seu time.');
      target.clubInv = target.clubInv || [];
      if (!target.clubInv.includes(club.id)) target.clubInv.push(club.id);
      save(); send(target.id, friendsMsg(target));
      send(target.id, { t: 'clubInvite', clubId: club.id, name: club.name, fromName: u.name });
      toast(uid, 'Convite enviado para ' + target.name + '.');
      break;
    }
    case 'clubAccept': {
      const club = db.clubs[msg.clubId];
      if (!club || !(u.clubInv || []).includes(club.id)) return err(uid, 'Convite inválido.');
      if (u.clubId) return err(uid, 'Saia do seu time atual antes.');
      if (club.members.length >= 11) return err(uid, 'Esse time já tem 11 jogadores.');
      u.clubInv = u.clubInv.filter(x => x !== club.id);
      club.members.push(uid); u.clubId = club.id; save(); pushClub(club.id); send(uid, friendsMsg(u));
      break;
    }
    case 'clubLeave': {
      const club = u.clubId && db.clubs[u.clubId]; if (!club) return;
      club.members = club.members.filter(x => x !== uid); u.clubId = null;
      if (!club.members.length) delete db.clubs[club.id];
      else { if (club.owner === uid) club.owner = club.members[0]; pushClub(club.id); }
      save(); send(uid, clubMsg(u)); send(uid, meMsg(u));
      break;
    }
    // --- salas ---
    case 'lobbyCreate': {
      if (matchOf.has(uid)) return;
      leaveLobby(uid);
      const l = { code: newCode(), mode: msg.mode === 'ranked' ? 'ranked' : 'custom', host: uid, members: [], settings: { dur: 300, diff: 'normal' }, status: 'lobby' };
      lobbies.set(l.code, l);
      joinLobby(uid, l);
      break;
    }
    case 'lobbyJoin': {
      const l = lobbies.get(clean(msg.code, 5).toUpperCase());
      if (!l) return err(uid, 'Sala não encontrada. Confira o código.');
      joinLobby(uid, l);
      break;
    }
    case 'lobbyLeave': leaveLobby(uid); break;
    case 'lobbyInvite':
      if (!myLobby) return;
      if (!target || !conns.has(target.id)) return err(uid, 'Esse amigo não está online.');
      send(target.id, { t: 'invite', fromName: u.name, code: myLobby.code, mode: myLobby.mode });
      toast(uid, 'Convite enviado para ' + target.name + '.');
      break;
    case 'lobbySet': {
      if (!myLobby || myLobby.status !== 'lobby') return;
      const me = myLobby.members.find(m => m.id === uid);
      if (msg.side === 0 || msg.side === 1) { if (myLobby.mode === 'custom') me.side = msg.side; }
      if (['ATA', 'MEI', 'ZAG', 'GOL'].includes(msg.pos)) { me.pos = msg.pos; u.pos = msg.pos; save(); }
      pushLobby(myLobby);
      break;
    }
    case 'lobbySettings':
      if (!myLobby || myLobby.host !== uid) return;
      if ([180, 300, 480].includes(msg.dur)) myLobby.settings.dur = msg.dur;
      if (['facil', 'normal', 'dificil'].includes(msg.diff)) myLobby.settings.diff = msg.diff;
      pushLobby(myLobby);
      break;
    case 'lobbyStart':
      if (!myLobby || myLobby.host !== uid || myLobby.mode !== 'custom' || myLobby.status !== 'lobby') return;
      startMatch(myLobby.members.map(m => ({ id: m.id, side: m.side, pos: m.pos })), uid, false, Object.assign({}, myLobby.settings), [myLobby.code]);
      break;
    case 'queueStart':
      if (!myLobby || myLobby.host !== uid || myLobby.mode !== 'ranked' || myLobby.status !== 'lobby') return;
      myLobby.status = 'queue'; queue.push(myLobby.code); pushLobby(myLobby); tryMatchmaking();
      break;
    case 'queueCancel':
      if (!myLobby || myLobby.host !== uid || myLobby.status !== 'queue') return;
      queue = queue.filter(x => x !== myLobby.code); myLobby.status = 'lobby'; pushLobby(myLobby);
      break;
    // --- partida ao vivo ---
    case 'relay': {
      const m = matches.get(matchOf.get(uid)); if (!m) return;
      const out = JSON.stringify({ t: 'relay', from: uid, d: msg.d });
      if (msg.to === 'host') { if (m.host !== uid) { const hc = conns.get(m.host); if (hc) hc.send(out); } }
      else if (m.host === uid) for (const id of m.members) if (id !== uid) { const cc = conns.get(id); if (cc) cc.send(out); }
      break;
    }
    case 'matchEnd': {
      const m = matches.get(matchOf.get(uid));
      if (!m || m.host !== uid || m.ended) return;
      const sc = Array.isArray(msg.sc) ? [parseInt(msg.sc[0]) || 0, parseInt(msg.sc[1]) || 0] : [0, 0];
      const changes = m.ranked ? applyElo(m, sc) : {};
      for (const h of m.humans) { send(h.id, { t: 'matchResult', ranked: m.ranked, changes, sc }); if (db.users[h.id]) send(h.id, meMsg(db.users[h.id])); }
      finishMatch(m);
      break;
    }
    case 'leaveMatch': leaveMatch(uid); break;
    // --- ranking ---
    case 'top': {
      const list = Object.values(db.users).filter(x => x.games > 0).sort((a, b) => b.rating - a.rating).slice(0, 100).map(pub);
      send(uid, { t: 'top', list });
      break;
    }
  }
}

server.on('upgrade', (req, socket) => {
  const c = wsAccept(req, socket); if (!c) return;
  c.onmessage = txt => { let msg; try { msg = JSON.parse(txt); } catch (e) { return; } try { handle(c, msg); } catch (e) { console.error(e); } };
  c.onclose = () => {
    const uid = c.uid;
    if (uid && conns.get(uid) === c) { conns.delete(uid); leaveMatch(uid); leaveLobby(uid); pushFriendsOf(uid); }
  };
});
server.listen(PORT, () => {
  console.log('==============================================');
  console.log(' Clube Pro 3D — servidor online ligado!');
  console.log(' Abra no navegador:  http://localhost:' + PORT);
  console.log('==============================================');
});
