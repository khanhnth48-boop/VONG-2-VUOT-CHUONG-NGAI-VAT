
import express from 'express';
import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { Server } from 'socket.io';
import pg from 'pg';

const { Pool } = pg;

if (!process.env.DATABASE_URL) {
  console.error('Missing DATABASE_URL.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5
});

pool.on('error', err => {
  console.error('PostgreSQL idle connection error:', err);
});

const app = express();
const http = createServer(app);
const io = new Server(http);


app.get('/20s.wav', (_req, res) =>
  res.sendFile('20s.wav', { root: process.cwd() })
);


app.get('/', (_req, res) =>
  res.sendFile('index.html', { root: process.cwd() })
);

const rooms = new Map();
const queues = new Map();

const clean = value =>
  String(value ?? '').trim().slice(0, 160);

const token = () =>
  randomBytes(24).toString('hex');

const roomCode = () =>
  randomBytes(4).toString('hex').slice(0, 6).toUpperCase();

const ROOM_GRACE_MS = 2 * 60 * 60 * 1000;
const TEAM_IDS = ['1', '2', '3', '4', '5', '6'];

function serialize(r) {
  return {
    code: r.code,
    hostToken: r.hostToken,
    teams: Object.fromEntries(
      Object.entries(r.teams).map(([id, t]) => [
        id,
        {
          name: t.name,
          token: t.token,
          answer: t.answer,
          submitted: t.submitted,
          score: t.score
        }
      ])
    ),
    question: r.question,
    number: r.number,
    open: r.open,
    seconds: r.seconds,
    deadline: r.deadline,
    revealed: r.revealed,
    hostDisconnectedAt: r.hostDisconnectedAt
  };
}

function hydrate(data) {
  return {
    ...data,
    teams: Object.fromEntries(
      Object.entries(data.teams || {}).map(([id, t]) => [
        id,
        { ...t, sid: null }
      ])
    ),
    hostSid: null,
    timer: null,
    cleanupTimer: null,
    hostDisconnectedAt: Date.now()
  };
}

async function save(r) {
  await pool.query(
    `INSERT INTO olympia_rooms (code, data, updated_at)
     VALUES ($1, $2::jsonb, NOW())
     ON CONFLICT (code)
     DO UPDATE SET
       data = EXCLUDED.data,
       updated_at = NOW()`,
    [r.code, JSON.stringify(serialize(r))]
  );
}


async function insertNewRoom(r) {
  const result = await pool.query(
    `INSERT INTO olympia_rooms (code, data, updated_at)
     VALUES ($1, $2::jsonb, NOW())
     ON CONFLICT (code) DO NOTHING
     RETURNING code`,
    [r.code, JSON.stringify(serialize(r))]
  );

  return result.rowCount === 1;
}
async function removeRoom(r) {
  await pool.query(
    'DELETE FROM olympia_rooms WHERE code = $1',
    [r.code]
  );

  clearTimeout(r.timer);
  clearTimeout(r.cleanupTimer);
  rooms.delete(r.code);
  io.to(r.code + ':players').emit('ended');
}

function enqueue(r, work) {
  const prior = queues.get(r.code) || Promise.resolve();
  const result = prior.catch(() => {}).then(work);
  const tail = result.catch(() => {});

  queues.set(r.code, tail);

  tail.then(() => {
    if (queues.get(r.code) === tail) {
      queues.delete(r.code);
    }
  });

  return result;
}

function snapshot(r) {
  return {
    code: r.code,
    question: r.question,
    number: r.number,
    open: r.open,
    seconds: r.seconds,
    deadline: r.deadline,
    revealed: r.revealed,
    hostConnected: Boolean(r.hostSid),
    teams: Object.fromEntries(
      Object.entries(r.teams).map(([id, t]) => [
        id,
        {
          name: t.name,
          connected: Boolean(t.sid),
          answer: t.answer,
          submitted: t.submitted,
          score: t.score
        }
      ])
    )
  };
}

function publish(r) {
  io.to(r.code + ':host').emit('state', snapshot(r));

  for (const t of Object.values(r.teams)) {
    if (!t.sid) continue;

    io.to(t.sid).emit('question', {
      number: r.number,
      question: r.question,
      open: r.open,
      seconds: r.seconds,
      deadline: r.deadline,
      revealed: r.revealed,
      submitted: t.submitted
    });
  }
}

function roomForHost(s) {
  const r = rooms.get(s.data.hostCode);
  return r?.hostSid === s.id ? r : null;
}

function respondError(cb, err) {
  console.error('Database operation failed:', err);
  cb?.({
    ok: false,
    error: 'Lỗi lưu dữ liệu. Vui lòng thử lại.'
  });
}

function scheduleQuestionEnd(r) {
  clearTimeout(r.timer);

  if (!r.open || !r.deadline) return;

  const questionNumber = r.number;
  const delay = Math.max(0, r.deadline - Date.now());

  r.timer = setTimeout(() => {
    enqueue(r, async () => {
      if (
        rooms.get(r.code) !== r ||
        r.number !== questionNumber ||
        !r.open
      ) return;

      if (Date.now() < r.deadline) {
        scheduleQuestionEnd(r);
        return;
      }

      const before = serialize(r);

      r.open = false;
      r.revealed = true;
      r.deadline = null;

      try {
        await save(r);
        publish(r);
      } catch (err) {
        Object.assign(r, {
          open: before.open,
          revealed: before.revealed,
          deadline: before.deadline
        });

        console.error(err);
        r.timer = setTimeout(
          () => scheduleQuestionEnd(r),
          1000
        );
      }
    }).catch(console.error);
  }, delay);
}

function expireLater(r) {
  clearTimeout(r.cleanupTimer);

  if (r.hostSid) return;

  const delay = Math.max(
    0,
    ROOM_GRACE_MS -
      (Date.now() - r.hostDisconnectedAt)
  );

  r.cleanupTimer = setTimeout(() => {
    enqueue(r, async () => {
      if (r.hostSid || rooms.get(r.code) !== r) return;

      try {
        await removeRoom(r);
      } catch (err) {
        console.error('Room cleanup failed:', err);
        r.cleanupTimer = setTimeout(
          () => expireLater(r),
          30000
        );
      }
    }).catch(console.error);
  }, delay);
}

io.on('connection', s => {

s.on('create', ({ code: requestedCode } = {}, cb) => {
  const customCode = String(requestedCode ?? '')
    .trim()
    .toUpperCase();

  if (customCode && !/^[A-Z0-9]{4,20}$/.test(customCode)) {
    return cb?.({
      ok: false,
      error: 'Mã phòng phải gồm 4–20 ký tự, chỉ chứa chữ và số.'
    });
  }

  let code = customCode || roomCode();

  if (customCode && rooms.has(code)) {
    return cb?.({
      ok: false,
      error: 'Mã phòng này đã tồn tại. Vui lòng chọn mã khác.'
    });
  }

  while (!customCode && rooms.has(code)) {
    code = roomCode();
  }

    const hostToken = token();

    const r = {
      code,
      hostToken,
      hostSid: null,
      teams: {},
      question: '',
      number: 0,
      open: false,
      seconds: 20,
      deadline: null,
      revealed: false,
      timer: null,
      cleanupTimer: null,
      hostDisconnectedAt: Date.now()
    };

    enqueue(r, async () => {
      

let created = await insertNewRoom(r);

if (!created && !customCode) {
  for (let attempt = 0; attempt < 9 && !created; attempt++) {
    code = roomCode();

    while (rooms.has(code)) {
      code = roomCode();
    }

    r.code = code;
    created = await insertNewRoom(r);
  }
}

if (!created) {
  return cb?.({
    ok: false,
    error: 'Mã phòng đã tồn tại. Vui lòng chọn mã khác.'
  });
}


      rooms.set(code, r);
      r.hostSid = s.id;
      s.data.hostCode = code;
      s.join(code + ':host');

      cb?.({
        ok: true,
        code,
        token: hostToken
      });

      publish(r);
    }).catch(err => respondError(cb, err));
  });

  s.on('hostJoin', ({ code, token: hostToken } = {}, cb) => {
    const r = rooms.get(clean(code).toUpperCase());

    if (!r || !hostToken || r.hostToken !== hostToken) {
      return cb?.({
        ok: false,
        error: 'Không khôi phục được phòng giáo viên.'
      });
    }

    enqueue(r, async () => {
      if (rooms.get(r.code) !== r) {
        return cb?.({
          ok: false,
          error: 'Phòng đã hết hạn.'
        });
      }

      if (r.hostSid && r.hostSid !== s.id) {
        io.sockets.sockets
          .get(r.hostSid)
          ?.disconnect(true);
      }

      r.hostSid = s.id;
      r.hostDisconnectedAt = null;

      clearTimeout(r.cleanupTimer);

      s.data.hostCode = r.code;
      s.join(r.code + ':host');

      cb?.({
        ok: true,
        code: r.code
      });

      publish(r);
    }).catch(err => respondError(cb, err));
  });

  s.on('join', ({ code, team, token: teamToken } = {}, cb) => {
    const r = rooms.get(clean(code).toUpperCase());
    const id = String(team);

    if (!r || !TEAM_IDS.includes(id)) {
      return cb?.({
        ok: false,
        error: 'Mã phòng hoặc số đội không hợp lệ.'
      });
    }

    enqueue(r, async () => {
      if (rooms.get(r.code) !== r) {
        return cb?.({
          ok: false,
          error: 'Phòng đã hết hạn.'
        });
      }

      let t = r.teams[id];

      if (!t) {
        t = {
          name: 'Đội ' + id,
          sid: null,
          token: token(),
          answer: '',
          submitted: false,
          score: 0
        };

        r.teams[id] = t;

        try {
          await save(r);
        } catch (err) {
          delete r.teams[id];
          throw err;
        }
      } else if (!teamToken || t.token !== teamToken) {
        return cb?.({
          ok: false,
          error: 'Đội này đã được đăng ký trên thiết bị khác.'
        });
      }

      if (t.sid && t.sid !== s.id) {
        io.sockets.sockets
          .get(t.sid)
          ?.disconnect(true);
      }

      t.sid = s.id;
      s.data.code = r.code;
      s.data.team = id;
      s.join(r.code + ':players');

      cb?.({
        ok: true,
        team: id,
        code: r.code,
        token: t.token,
        submitted: t.submitted
      });

      publish(r);
    }).catch(err => respondError(cb, err));
  });

  s.on('openQuestion', ({ question, seconds } = {}, cb) => {
    const r = roomForHost(s);

    if (!r) {
      return cb?.({
        ok: false,
        error: 'Không tìm thấy phòng giáo viên.'
      });
    }

    enqueue(r, async () => {
      if (roomForHost(s) !== r) {
        return cb?.({
          ok: false,
          error: 'Không tìm thấy phòng giáo viên.'
        });
      }

      const q = clean(question);

      if (!q) {
        return cb?.({
          ok: false,
          error: 'Chưa nhập câu hỏi.'
        });
      }

      const previous = serialize(r);

      r.number++;
      r.question = q;
      r.seconds = Math.max(
        5,
        Math.min(300, Number(seconds) || 20)
      );
      r.deadline = Date.now() + r.seconds * 1000;
      r.open = true;
      r.revealed = false;

      for (const t of Object.values(r.teams)) {
        t.answer = '';
        t.submitted = false;
      }

      try {
        await save(r);
      } catch (err) {
        Object.assign(r, {
          question: previous.question,
          number: previous.number,
          seconds: previous.seconds,
          deadline: previous.deadline,
          open: previous.open,
          revealed: previous.revealed
        });

        for (const [id, t] of Object.entries(r.teams)) {
          t.answer = previous.teams[id]?.answer ?? '';
          t.submitted =
            previous.teams[id]?.submitted ?? false;
        }

        throw err;
      }

      scheduleQuestionEnd(r);
      publish(r);
      cb?.({ ok: true });
    }).catch(err => respondError(cb, err));
  });

  s.on('submit', ({ answer } = {}, cb) => {
    const r = rooms.get(s.data.code);

    if (!r) {
      return cb?.({
        ok: false,
        error: 'Không tìm thấy phòng.'
      });
    }

    enqueue(r, async () => {
      const t = r.teams[s.data.team];

      if (
        !t ||
        t.sid !== s.id ||
        !r.open ||
        !r.deadline ||
        Date.now() >= r.deadline ||
        t.submitted
      ) {
        return cb?.({
          ok: false,
          error: 'Đã hết giờ hoặc đội đã gửi đáp án.'
        });
      }

      const value = clean(answer);

      if (!value) {
        return cb?.({
          ok: false,
          error: 'Chưa nhập đáp án.'
        });
      }

      const previous = {
        answer: t.answer,
        submitted: t.submitted
      };

      t.answer = value;
      t.submitted = true;

      try {
        await save(r);
      } catch (err) {
        Object.assign(t, previous);
        throw err;
      }

      cb?.({ ok: true });
      publish(r);
    }).catch(err => respondError(cb, err));
  });

  s.on('reveal', (_unused, cb) => {
    const r = roomForHost(s);

    if (!r) {
      return cb?.({
        ok: false,
        error: 'Không tìm thấy phòng.'
      });
    }

    enqueue(r, async () => {
      if (roomForHost(s) !== r) {
        return cb?.({
          ok: false,
          error: 'Không tìm thấy phòng.'
        });
      }

      const previous = {
        open: r.open,
        revealed: r.revealed,
        deadline: r.deadline
      };

      r.open = false;
      r.revealed = true;
      r.deadline = null;

      try {
        await save(r);
      } catch (err) {
        Object.assign(r, previous);
        throw err;
      }

      clearTimeout(r.timer);
      publish(r);
      cb?.({ ok: true });
    }).catch(err => respondError(cb, err));
  });


s.on('score', ({ team, delta } = {}, cb) => {
  const r = roomForHost(s);

  if (!r) {
    return cb?.({ ok: false });
  }

  enqueue(r, async () => {
    if (roomForHost(s) !== r) {
      return cb?.({ ok: false });
    }

    const t = r.teams[String(team)];
    const change = Number(delta);

    // Chỉ cho phép cộng 20 hoặc hoàn tác trừ 20.
    if (!t || ![20, -20].includes(change)) {
      return cb?.({
        ok: false,
        error: 'Thao tác điểm không hợp lệ.'
      });
    }

    // Không cho phép điểm âm.
    if (change === -20 && t.score < 20) {
      return cb?.({
        ok: false,
        error: 'Không đủ điểm để hoàn tác.'
      });
    }

    const previousScore = t.score;
    t.score += change;

    try {
      await save(r);
    } catch (err) {
      t.score = previousScore;
      throw err;
    }

    publish(r);
    cb?.({ ok: true });
  }).catch(err => respondError(cb, err));
});


  s.on('disconnect', () => {
    if (s.data.code) {
      const r = rooms.get(s.data.code);
      const t = r?.teams[s.data.team];

      if (t?.sid === s.id) {
        t.sid = null;
        publish(r);
      }
    }

    if (s.data.hostCode) {
      const r = rooms.get(s.data.hostCode);

      if (r?.hostSid === s.id) {
        r.hostSid = null;
        r.hostDisconnectedAt = Date.now();
        publish(r);

        enqueue(r, async () => {
          if (r.hostSid) return;
          await save(r);
          expireLater(r);
        }).catch(err => {
          console.error('Host disconnect save failed:', err);
          expireLater(r);
        });
      }
    }
  });
});

async function start() {
  await pool.query('SELECT 1');

  await pool.query(`
    CREATE TABLE IF NOT EXISTS olympia_rooms (
      code TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  const result = await pool.query(
    'SELECT data FROM olympia_rooms'
  );

  for (const row of result.rows) {
    const r = hydrate(row.data);

    if (
      r.open &&
      (!r.deadline || Date.now() >= r.deadline)
    ) {
      r.open = false;
      r.revealed = true;
      r.deadline = null;
      await save(r);
    }

    rooms.set(r.code, r);
    scheduleQuestionEnd(r);
    expireLater(r);
  }

  http.listen(
    process.env.PORT || 3000,
    () => console.log(
      `Olympia running with PostgreSQL (${rooms.size} rooms restored)`
    )
  );
}

start().catch(err => {
  console.error('Database startup failed:', err);
  process.exit(1);
});
