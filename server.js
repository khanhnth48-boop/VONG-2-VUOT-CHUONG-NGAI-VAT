import express from 'express';
import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { Server } from 'socket.io';

const app = express();
const http = createServer(app);
const io = new Server(http);
app.use(express.static('public'));
app.get('/', (_req, res) => res.sendFile('index.html', { root: process.cwd() }));

const rooms = new Map();
const clean = value => String(value ?? '').trim().slice(0, 160);
const token = () => randomBytes(24).toString('hex');
const roomCode = () => randomBytes(4).toString('hex').slice(0, 6).toUpperCase();
const ROOM_GRACE_MS = 2 * 60 * 60 * 1000;

function snapshot(r) {
  return {
    code: r.code, question: r.question, number: r.number,
    open: r.open, seconds: r.seconds, deadline: r.deadline,
    revealed: r.revealed, hostConnected: Boolean(r.hostSid),
    teams: Object.fromEntries(Object.entries(r.teams).map(([id, t]) => [id, {
      name: t.name, connected: Boolean(t.sid), answer: t.answer,
      submitted: t.submitted, score: t.score
    }]))
  };
}
function publish(r) {
  io.to(r.code + ':host').emit('state', snapshot(r));
  io.to(r.code + ':players').emit('question', {
    number: r.number, question: r.question, open: r.open,
    seconds: r.seconds, deadline: r.deadline, revealed: r.revealed
  });
}
function roomForHost(s) {
  const r = rooms.get(s.data.hostCode);
  return r?.hostSid === s.id ? r : null;
}
function expireLater(r) {
  clearTimeout(r.cleanupTimer);
  if (r.hostSid) return;
  r.cleanupTimer = setTimeout(() => {
    if (r.hostSid || rooms.get(r.code) !== r) return;
    clearTimeout(r.timer);
    io.to(r.code + ':players').emit('ended');
    rooms.delete(r.code);
  }, ROOM_GRACE_MS);
}

io.on('connection', s => {
  s.on('create', (_unused, cb) => {
    let code = roomCode();
    while (rooms.has(code)) code = roomCode();
    const hostToken = token();
    const r = {
      code, hostToken, hostSid: s.id, teams: {}, question: '', number: 0,
      open: false, seconds: 20, deadline: null, revealed: false,
      timer: null, cleanupTimer: null
    };
    rooms.set(code, r);
    s.data.hostCode = code;
    s.join(code + ':host');
    cb?.({ ok: true, code, token: hostToken });
    publish(r);
  });

  s.on('hostJoin', ({ code, token: hostToken } = {}, cb) => {
    const r = rooms.get(clean(code).toUpperCase());
    if (!r || !hostToken || r.hostToken !== hostToken)
      return cb?.({ ok: false, error: 'Không khôi phục được phòng giáo viên.' });
    if (r.hostSid && r.hostSid !== s.id) {
      io.sockets.sockets.get(r.hostSid)?.disconnect(true);
    }
    r.hostSid = s.id;
    clearTimeout(r.cleanupTimer);
    s.data.hostCode = r.code;
    s.join(r.code + ':host');
    cb?.({ ok: true, code: r.code });
    publish(r);
  });

  s.on('join', ({ code, team, token: teamToken } = {}, cb) => {
    const r = rooms.get(clean(code).toUpperCase());
    const id = String(team);
    if (!r || !['1','2','3','4','5','6'].includes(id))
      return cb?.({ ok: false, error: 'Mã phòng hoặc số đội không hợp lệ.' });
    let t = r.teams[id];
    if (!t) {
      t = { name: 'Đội ' + id, sid: null, token: token(), answer: '', submitted: false, score: 0 };
      r.teams[id] = t;
    } else if (!teamToken || t.token !== teamToken) {
      return cb?.({ ok: false, error: 'Đội này đã được đăng ký trên thiết bị khác.' });
    }
    if (t.sid && t.sid !== s.id) io.sockets.sockets.get(t.sid)?.disconnect(true);
    t.sid = s.id;
    s.data.code = r.code;
    s.data.team = id;
    s.join(r.code + ':players');
    cb?.({ ok: true, team: id, code: r.code, token: t.token, submitted: t.submitted });
    publish(r);
  });

  s.on('openQuestion', ({ question, seconds } = {}, cb) => {
    const r = roomForHost(s);
    if (!r) return cb?.({ ok: false, error: 'Không tìm thấy phòng giáo viên.' });
    const q = clean(question);
    if (!q) return cb?.({ ok: false, error: 'Chưa nhập câu hỏi.' });
    clearTimeout(r.timer);
    r.number++;
    r.question = q;
    r.seconds = Math.max(5, Math.min(300, Number(seconds) || 20));
    r.deadline = Date.now() + r.seconds * 1000;
    r.open = true;
    r.revealed = false;
    for (const t of Object.values(r.teams)) { t.answer = ''; t.submitted = false; }
    const questionNumber = r.number;
    r.timer = setTimeout(() => {
      if (rooms.get(r.code) !== r || r.number !== questionNumber || !r.open) return;
      r.open = false;
      r.revealed = true;
      r.deadline = null;
      publish(r);
    }, r.seconds * 1000);
    publish(r);
    cb?.({ ok: true });
  });

  s.on('submit', ({ answer } = {}, cb) => {
    const r = rooms.get(s.data.code);
    const t = r?.teams[s.data.team];
    if (!r || !t || t.sid !== s.id || !r.open || !r.deadline ||
        Date.now() >= r.deadline || t.submitted)
      return cb?.({ ok: false, error: 'Đã hết giờ hoặc đội đã gửi đáp án.' });
    const value = clean(answer);
    if (!value) return cb?.({ ok: false, error: 'Chưa nhập đáp án.' });
    t.answer = value;
    t.submitted = true;
    cb?.({ ok: true });
    publish(r);
  });

  s.on('reveal', (_unused, cb) => {
    const r = roomForHost(s);
    if (!r) return cb?.({ ok: false, error: 'Không tìm thấy phòng.' });
    clearTimeout(r.timer);
    r.open = false;
    r.revealed = true;
    r.deadline = null;
    publish(r);
    cb?.({ ok: true });
  });

  s.on('score', ({ team, delta } = {}, cb) => {
    const r = roomForHost(s);
    const t = r?.teams[String(team)];
    if (!t) return cb?.({ ok: false });
    // Only the intended +20 action is allowed.
    if (Number(delta) !== 20) return cb?.({ ok: false });
    t.score += 20;
    publish(r);
    cb?.({ ok: true });
  });

  s.on('disconnect', () => {
    if (s.data.code) {
      const r = rooms.get(s.data.code);
      const t = r?.teams[s.data.team];
      if (t?.sid === s.id) { t.sid = null; publish(r); }
    }
    if (s.data.hostCode) {
      const r = rooms.get(s.data.hostCode);
      if (r?.hostSid === s.id) {
        r.hostSid = null;
        publish(r);
        expireLater(r);
      }
    }
  });
});

http.listen(process.env.PORT || 3000, () => console.log('Olympia running'));
