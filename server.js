// ============================================================
//  wc-overlay : 월드컵 중계 스타일 vMix 오버레이 (라이브 데이터 + OSC)
//   - http://localhost:8093/          → 오버레이 (vMix 브라우저 소스)
//   - http://localhost:8093/control   → 컨트롤 페이지
//   - SSE(/events)로 실시간 동기화 · 외부 npm 의존성 없음
//   - OSC: Node 내장 dgram(UDP)로 메시지 전송
//   - 스포츠 데이터: football-data.org 프록시 (토큰은 컨트롤에서 입력)
//   - 뉴스: 구글뉴스 RSS('월드컵' 등 키워드) → 헤드라인
// ============================================================
const http = require("http");
const fs = require("fs");
const path = require("path");
const dgram = require("dgram");

const PORT = process.env.PORT || 8093;
const DIR = __dirname;
const STATE_FILE = path.join(DIR, ".state.json");

// ── 기본 상태 ───────────────────────────────────────────────
const DEFAULT_STATE = {
  editMode: false, // true면 오버레이에서 드래그로 위치 이동 가능
  layout: {
    scoreboard: { x: 90, y: 60, scale: 1 },
    goal:       { x: 590, y: 355, scale: 1 },
    crew:       { x: 90, y: 850, scale: 1 },
    ticker:     { x: 90, y: 954, scale: 1 },
    goallog:    { x: 1480, y: 320, scale: 1 },
  },
  scoreboard: {
    show: true,
    title: "2026 FIFA 월드컵™",
    round: "16강",
    homeName: "대한민국", homeFlag: "🇰🇷", homeScore: 0,
    awayName: "포르투갈", awayFlag: "🇵🇹", awayScore: 0,
  },
  timer: {
    running: false,
    half: 1,                 // 1=전반, 2=후반
    elapsedSec: 0,           // 정지 시점까지 누적 초
    startedAt: null,         // 진행 중이면 시작 epoch(ms)
    label1: "전반", label2: "후반",
  },
  goal: {
    show: false,
    headline: "골! 대한민국",
    minute: "0'",
    autoHideSec: 6,          // 0이면 수동
  },
  goallog: {
    show: true,
    title: "득점 기록",
    items: [],               // {team:'home'|'away', teamName, minute, scorer}
  },
  crew: {
    show: true,
    caster: "배성재", casterRole: "캐스터",
    analyst: "박지성", analystRole: "해설위원",
  },
  ticker: {
    show: true,
    label: "주요뉴스",
    mode: "auto",            // auto=구글뉴스RSS / manual=직접입력
    query: "월드컵",
    text: "'캡틴' 손흥민, 2026 북중미 월드컵 출격 준비 완료",
    headlines: [],
  },
  osc: {
    enabled: false,
    host: "127.0.0.1",
    port: 9000,
    addrHome: "/goal/home",
    addrAway: "/goal/away",
  },
  api: {
    token: "",
    competition: "WC",       // football-data 대회 코드 (월드컵=WC)
    matchId: "",
    liveSync: false,
  },
};

function structuredCloneSafe(o) { return JSON.parse(JSON.stringify(o)); }

function loadState() {
  try {
    const saved = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    return deepMerge(structuredCloneSafe(DEFAULT_STATE), saved);
  } catch {
    return structuredCloneSafe(DEFAULT_STATE);
  }
}
function saveState(s) { try { fs.writeFileSync(STATE_FILE, JSON.stringify(s, null, 2)); } catch {} }
function deepMerge(base, patch) {
  for (const k of Object.keys(patch || {})) {
    const v = patch[k];
    if (v && typeof v === "object" && !Array.isArray(v)) base[k] = deepMerge(base[k] || {}, v);
    else base[k] = v;
  }
  return base;
}

let state = loadState();
// 재시작 시 타이머는 멈춘 상태로
state.timer.running = false; state.timer.startedAt = null;

const clients = new Set();
function broadcast() {
  const payload = `data: ${JSON.stringify(state)}\n\n`;
  for (const res of clients) { try { res.write(payload); } catch {} }
}

// ── 타이머 계산 ──────────────────────────────────────────────
function currentSec() {
  const t = state.timer;
  return t.elapsedSec + (t.running && t.startedAt ? (Date.now() - t.startedAt) / 1000 : 0);
}
// 골 표기용 분 (전반 0+, 후반 45+)
function currentMinuteLabel() {
  const base = state.timer.half === 2 ? 45 : 0;
  const m = base + Math.floor(currentSec() / 60) + 1;
  return `${m}'`;
}

// ── OSC (메시지만, 인자 없음) ─────────────────────────────────
const oscSock = dgram.createSocket("udp4");
function oscString(s) {
  const buf = Buffer.from(String(s), "ascii");
  const withNul = Buffer.concat([buf, Buffer.from([0])]);
  const pad = (4 - (withNul.length % 4)) % 4;
  return Buffer.concat([withNul, Buffer.alloc(pad)]);
}
function sendOSC(addr) {
  if (!state.osc.enabled || !addr) return;
  try {
    const packet = Buffer.concat([oscString(addr), oscString(",")]); // 주소 + 빈 타입태그
    oscSock.send(packet, state.osc.port, state.osc.host, (e) => {
      if (e) console.log("OSC 전송 실패:", e.message);
    });
    console.log(`OSC → ${state.osc.host}:${state.osc.port}  ${addr}`);
  } catch (e) { console.log("OSC 오류:", e.message); }
}

// ── 득점 처리 ────────────────────────────────────────────────
let goalHideTimer = null;
function applyScore(team, delta, scorer) {
  const key = team === "home" ? "homeScore" : "awayScore";
  let v = Number(state.scoreboard[key]) || 0;
  v = Math.max(0, v + delta);
  state.scoreboard[key] = v;
  const teamName = team === "home" ? state.scoreboard.homeName : state.scoreboard.awayName;

  if (delta > 0) {
    const minute = currentMinuteLabel();
    // 골 알림 (선수명 불필요, 타이머 시간 기준)
    state.goal.headline = `골! ${teamName}`;
    state.goal.minute = minute;
    state.goal.show = true;
    // 득점 기록
    state.goallog.items.push({ team, teamName, minute, scorer: scorer || "" });
    // OSC (메시지만)
    sendOSC(team === "home" ? state.osc.addrHome : state.osc.addrAway);
    // 자동 숨김
    if (goalHideTimer) clearTimeout(goalHideTimer);
    if (state.goal.autoHideSec > 0) {
      goalHideTimer = setTimeout(() => { state.goal.show = false; saveState(state); broadcast(); },
        state.goal.autoHideSec * 1000);
    }
  } else {
    // -1: 해당 팀 마지막 기록 제거 (있으면)
    for (let i = state.goallog.items.length - 1; i >= 0; i--) {
      if (state.goallog.items[i].team === team) { state.goallog.items.splice(i, 1); break; }
    }
  }
  saveState(state); broadcast();
}

// ── 뉴스 (구글뉴스 RSS) ──────────────────────────────────────
async function fetchHeadlines(query) {
  const url = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=ko&gl=KR&ceid=KR:ko`;
  const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" } });
  const xml = await r.text();
  const items = [...xml.matchAll(/<item>([\s\S]*?)<\/item>/g)].slice(0, 12).map((m) => {
    const t = (m[1].match(/<title>([\s\S]*?)<\/title>/) || [])[1] || "";
    return t.replace(/<!\[CDATA\[|\]\]>/g, "").replace(/&quot;/g, '"').replace(/&apos;/g, "'")
            .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
            .replace(/\s*-\s*[^-]+$/, "").trim();   // 끝의 "- 언론사" 제거
  }).filter(Boolean);
  return items;
}
async function refreshNews() {
  if (state.ticker.mode !== "auto") return;
  try {
    const items = await fetchHeadlines(state.ticker.query || "월드컵");
    if (items.length) {
      state.ticker.headlines = items;
      state.ticker.text = items.join("        ·        ");
      saveState(state); broadcast();
      console.log(`뉴스 갱신: ${items.length}건`);
    }
  } catch (e) { console.log("뉴스 갱신 실패:", e.message); }
}

// ── football-data.org 프록시 ─────────────────────────────────
async function fdFetch(apiPath) {
  if (!state.api.token) throw new Error("API 토큰이 없습니다 (컨트롤에서 입력하세요).");
  const r = await fetch("https://api.football-data.org/v4" + apiPath, {
    headers: { "X-Auth-Token": state.api.token },
  });
  if (!r.ok) throw new Error(`football-data ${r.status} ${r.statusText}`);
  return r.json();
}
function simplifyMatch(m) {
  return {
    id: m.id, utcDate: m.utcDate, status: m.status,
    stage: m.stage, group: m.group,
    home: { name: m.homeTeam?.name, tla: m.homeTeam?.tla, crest: m.homeTeam?.crest },
    away: { name: m.awayTeam?.name, tla: m.awayTeam?.tla, crest: m.awayTeam?.crest },
    score: { home: m.score?.fullTime?.home, away: m.score?.fullTime?.away },
  };
}
async function loadMatches() {
  const data = await fdFetch(`/competitions/${state.api.competition || "WC"}/matches`);
  return (data.matches || []).map(simplifyMatch);
}
async function selectMatch(matchId) {
  const m = await fdFetch(`/matches/${matchId}`);
  state.api.matchId = String(matchId);
  state.scoreboard.homeName = m.homeTeam?.name || state.scoreboard.homeName;
  state.scoreboard.awayName = m.awayTeam?.name || state.scoreboard.awayName;
  if (m.homeTeam?.crest) state.scoreboard.homeFlag = m.homeTeam.crest;
  if (m.awayTeam?.crest) state.scoreboard.awayFlag = m.awayTeam.crest;
  if (m.score?.fullTime?.home != null) state.scoreboard.homeScore = m.score.fullTime.home;
  if (m.score?.fullTime?.away != null) state.scoreboard.awayScore = m.score.fullTime.away;
  applyMatchGoals(m);
  saveState(state); broadcast();
  return m;
}
function applyMatchGoals(m) {
  if (!Array.isArray(m.goals)) return;
  const homeId = m.homeTeam?.id;
  state.goallog.items = m.goals.map((g) => {
    const team = g.team?.id === homeId ? "home" : "away";
    return {
      team,
      teamName: team === "home" ? state.scoreboard.homeName : state.scoreboard.awayName,
      minute: (g.minute != null ? g.minute : "") + (g.minute != null ? "'" : ""),
      scorer: g.scorer?.name || "",
    };
  });
}
async function syncMatch() {
  if (!state.api.matchId) return;
  const m = await fdFetch(`/matches/${state.api.matchId}`);
  if (m.score?.fullTime?.home != null) state.scoreboard.homeScore = m.score.fullTime.home;
  if (m.score?.fullTime?.away != null) state.scoreboard.awayScore = m.score.fullTime.away;
  applyMatchGoals(m);
  saveState(state); broadcast();
}

// ── HTTP ─────────────────────────────────────────────────────
const NOCACHE = { "Cache-Control": "no-store, no-cache, must-revalidate", Pragma: "no-cache", Expires: "0" };
function serveFile(res, file, type) {
  try { res.writeHead(200, { "Content-Type": type, ...NOCACHE }); res.end(fs.readFileSync(path.join(DIR, "public", file))); }
  catch { res.writeHead(404).end("not found"); }
}
function readBody(req) {
  return new Promise((resolve) => { let b = ""; req.on("data", (c) => (b += c)); req.on("end", () => resolve(b)); });
}
function json(res, code, obj) { res.writeHead(code, { "Content-Type": "application/json; charset=utf-8", ...NOCACHE }); res.end(JSON.stringify(obj)); }

const server = http.createServer(async (req, res) => {
  const url = (req.url || "/").split("?")[0];
  try {
    if (url === "/" || url === "/overlay" || url === "/overlay.html") return serveFile(res, "overlay.html", "text/html; charset=utf-8");
    if (url === "/control" || url === "/control.html") return serveFile(res, "control.html", "text/html; charset=utf-8");
    if (url === "/state") return json(res, 200, withClock(state));

    if (url === "/events") {
      res.writeHead(200, { "Content-Type": "text/event-stream", Connection: "keep-alive", ...NOCACHE });
      res.write(`data: ${JSON.stringify(withClock(state))}\n\n`);
      clients.add(res);
      const ping = setInterval(() => { try { res.write(": ping\n\n"); } catch {} }, 15000);
      req.on("close", () => { clearInterval(ping); clients.delete(res); });
      return;
    }

    if (url === "/update" && req.method === "POST") {
      const patch = JSON.parse((await readBody(req)) || "{}");
      if (patch.__reset) { state = structuredCloneSafe(DEFAULT_STATE); }
      else state = deepMerge(state, patch);
      saveState(state); broadcast();
      return json(res, 200, { ok: true });
    }

    if (url === "/score" && req.method === "POST") {
      const { team, delta, scorer } = JSON.parse((await readBody(req)) || "{}");
      applyScore(team === "away" ? "away" : "home", Number(delta) || 0, scorer);
      return json(res, 200, { ok: true });
    }

    if (url === "/timer" && req.method === "POST") {
      const { cmd, half } = JSON.parse((await readBody(req)) || "{}");
      const t = state.timer;
      if (cmd === "start" && !t.running) { t.startedAt = Date.now(); t.running = true; }
      else if (cmd === "pause" && t.running) { t.elapsedSec = currentSec(); t.running = false; t.startedAt = null; }
      else if (cmd === "reset") { t.elapsedSec = 0; t.running = false; t.startedAt = null; }
      else if (cmd === "half") { if (half === 1 || half === 2) t.half = half; }
      saveState(state); broadcast();
      return json(res, 200, { ok: true, timer: t });
    }

    if (url === "/news/refresh" && req.method === "POST") {
      await refreshNews();
      return json(res, 200, { ok: true, headlines: state.ticker.headlines });
    }

    if (url === "/api/matches" && req.method === "POST") {
      const body = JSON.parse((await readBody(req)) || "{}");
      if (body.token != null) state.api.token = body.token;
      if (body.competition) state.api.competition = body.competition;
      saveState(state);
      try { const matches = await loadMatches(); return json(res, 200, { ok: true, matches }); }
      catch (e) { return json(res, 200, { ok: false, error: e.message }); }
    }
    if (url === "/api/select" && req.method === "POST") {
      const { matchId } = JSON.parse((await readBody(req)) || "{}");
      try { await selectMatch(matchId); return json(res, 200, { ok: true }); }
      catch (e) { return json(res, 200, { ok: false, error: e.message }); }
    }
    if (url === "/api/sync" && req.method === "POST") {
      try { await syncMatch(); return json(res, 200, { ok: true }); }
      catch (e) { return json(res, 200, { ok: false, error: e.message }); }
    }

    res.writeHead(404).end("not found");
  } catch (e) {
    json(res, 500, { ok: false, error: String(e && e.message || e) });
  }
});

// 오버레이/컨트롤에 내려줄 때 계산된 시계/초 포함
function withClock(s) {
  const sec = currentSec();
  const mm = String(Math.floor(sec / 60)).padStart(2, "0");
  const ss = String(Math.floor(sec % 60)).padStart(2, "0");
  const label = s.timer.half === 2 ? s.timer.label2 : s.timer.label1;
  return { ...s, _clock: { sec, text: `${label} ${mm}:${ss}`, running: s.timer.running, startedAt: s.timer.startedAt, elapsedSec: s.timer.elapsedSec } };
}

// 라이브 동기화 + 뉴스 주기 갱신
setInterval(() => { if (state.api.liveSync && state.api.matchId) syncMatch().catch(() => {}); }, 20000);
setInterval(() => refreshNews().catch(() => {}), 5 * 60 * 1000);

server.listen(PORT, () => {
  console.log(`\n  ⚽  wc-overlay 실행 중`);
  console.log(`  ──────────────────────────────────────────`);
  console.log(`  오버레이 (vMix):  http://localhost:${PORT}/`);
  console.log(`  컨트롤 페이지  :  http://localhost:${PORT}/control`);
  console.log(`  ──────────────────────────────────────────\n`);
  refreshNews().catch(() => {});
});
