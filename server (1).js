const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const GID = process.env.GOOGLE_CLIENT_ID || ''; // set this to require verified Google sign-in
const DATA = path.join(__dirname, 'data.json');

let db = { users: {}, messages: [], notes: [] };
try { db = Object.assign(db, JSON.parse(fs.readFileSync(DATA, 'utf8'))); } catch (e) {}
let tmr;
const save = () => { clearTimeout(tmr); tmr = setTimeout(() => fs.writeFile(DATA, JSON.stringify(db), () => {}), 400); };
const flush = () => { try { fs.writeFileSync(DATA, JSON.stringify(db)); } catch (e) {} process.exit(); };
process.on('SIGINT', flush); process.on('SIGTERM', flush);
const uid = () => Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
const clean = (s, n) => String(s || '').replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '').slice(0, n);

const server = http.createServer((req, res) => {
  if (req.url === '/config') {
    res.setHeader('Content-Type', 'application/json');
    return res.end(JSON.stringify({ googleClientId: GID }));
  }
  if (req.url === '/' || req.url.startsWith('/?')) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
  }
  res.writeHead(404); res.end('Not found');
});

const wss = new WebSocketServer({ server });
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const all = o => wss.clients.forEach(c => c.user && send(c, o));
const online = () => [...new Set([...wss.clients].filter(c => c.user).map(c => c.user.name))];

async function verifyGoogle(token) {
  const r = await fetch('https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(token));
  if (!r.ok) throw new Error('Google sign-in failed. Try again.');
  const j = await r.json();
  const verified = j.email_verified === true || j.email_verified === 'true';
  if (j.aud !== GID || !verified) throw new Error('Google sign-in was rejected.');
  if (!/@gmail\.com$/i.test(j.email || '')) throw new Error('Use a Gmail account.');
  return { email: j.email.toLowerCase(), name: clean(j.given_name || j.name, 24) };
}

async function join(ws, m) {
  let email, name = clean(m.username, 24).trim();
  if (m.idToken && GID) {
    const g = await verifyGoogle(m.idToken);
    email = g.email; name = name || g.name;
  } else {
    if (GID) throw new Error('Sign in with Google to continue.');
    email = clean(m.gmail, 80).trim().toLowerCase();
    if (!/^[a-z0-9._%+-]+@gmail\.com$/.test(email)) throw new Error('Enter a valid Gmail address.');
  }
  if (!db.users[email]) {
    if (name.length < 2) throw new Error('Choose a username with 2 to 24 characters.');
    if (Object.values(db.users).some(u => u.name.toLowerCase() === name.toLowerCase()))
      throw new Error('That username is taken. Choose another.');
    db.users[email] = { name, created: Date.now() };
    save();
  }
  ws.user = { email, name: db.users[email].name };
  send(ws, { t: 'init', me: ws.user.name, messages: db.messages, notes: db.notes, online: online() });
  all({ t: 'online', online: online() });
}

wss.on('connection', ws => {
  ws.alive = true; ws.hits = [];
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', async raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    try {
      if (m.t === 'join') return await join(ws, m);
      if (!ws.user) throw new Error('Join first.');
      const now = Date.now();
      ws.hits = ws.hits.filter(h => now - h < 10000);
      if (ws.hits.push(now) > 60) throw new Error('Slow down.');
      const by = ws.user.name;
      if (m.t === 'msg') {
        const text = clean(m.text, 500).trim(); if (!text) return;
        const o = { id: uid(), user: by, text, ts: now };
        db.messages = [...db.messages, o].slice(-200); save();
        all({ t: 'msg', m: o });
      } else if (m.t === 'note_new') {
        if (db.notes.length >= 500) throw new Error('Note limit reached.');
        const n = { id: uid(), title: '', body: '', by, ts: now };
        db.notes.unshift(n); save();
        all({ t: 'note', n }); send(ws, { t: 'opened', id: n.id });
      } else if (m.t === 'note_save') {
        const n = db.notes.find(x => x.id === m.id); if (!n) return;
        n.title = clean(m.title, 80); n.body = clean(m.body, 20000); n.by = by; n.ts = now; save();
        all({ t: 'note', n });
      } else if (m.t === 'note_del') {
        db.notes = db.notes.filter(x => x.id !== m.id); save();
        all({ t: 'note_del', id: m.id });
      }
    } catch (e) { send(ws, { t: 'err', msg: e.message }); }
  });
  ws.on('close', () => all({ t: 'online', online: online() }));
});

setInterval(() => wss.clients.forEach(ws => {
  if (!ws.alive) return ws.terminate();
  ws.alive = false; ws.ping();
}), 30000);

server.listen(PORT, () => console.log('Live Notes & Chat running on http://localhost:' + PORT));
