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
const os = require("os");
let watcher = null;   // 위플랩 별풍선 감시기(puppeteer) — 설치돼 있을 때만 로드
try { watcher = require("./watcher"); } catch (e) { console.log("위플랩 감시 비활성(puppeteer 미설치):", e.message); }

const PORT = process.env.PORT || 8093;
const DIR = __dirname;

// 같은 네트워크의 다른 PC에서 접속할 LAN IP (예: 192.168.0.x)
function lanIP() {
  const ifs = os.networkInterfaces();
  // en0(보통 Wi-Fi/이더넷) 우선, 없으면 첫 IPv4 비내부 주소
  const cands = [];
  for (const [name, addrs] of Object.entries(ifs)) {
    for (const a of addrs || []) {
      if (a.family === "IPv4" && !a.internal) cands.push({ name, address: a.address });
    }
  }
  const en = cands.find((c) => c.name === "en0") || cands.find((c) => /^en|eth|wl/.test(c.name)) || cands[0];
  return en ? en.address : "localhost";
}
const LAN_IP = lanIP();
let activePort = Number(PORT) || 8093;   // 포트 충돌 시 자동으로 다음 포트 사용
// 설치형(exe)에서는 설치 폴더가 읽기전용 → 쓰기 가능 폴더(WC_DATA_DIR)에 상태/업로드 저장
const DATA_DIR = process.env.WC_DATA_DIR || DIR;
const STATE_FILE = path.join(DATA_DIR, ".state.json");
const SOUNDS_BUNDLED = path.join(DIR, "public", "sounds");      // 동봉 사운드(읽기)
const SOUNDS_UPLOAD = path.join(DATA_DIR, "sounds");            // 업로드 사운드(쓰기)
try { fs.mkdirSync(SOUNDS_UPLOAD, { recursive: true }); } catch {}

// ── 기본 상태 ───────────────────────────────────────────────
const DEFAULT_STATE = {
  editMode: false, // true면 오버레이에서 드래그로 위치 이동 가능
  layout: {
    scoreboard: { x: 90, y: 60, scale: 1 },
    goal:       { x: 590, y: 355, scale: 1 },
    predict:    { x: 1560, y: 110, scale: 1 },
    goallog:    { x: 90, y: 620, scale: 1 },
    ticker:     { x: 90, y: 954, scale: 1 },
    formation:  { x: 360, y: 200, scale: 1 },
    donation:   { x: 580, y: 250, scale: 1 },
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
    autoHideSec: 3,          // 득점 후 3초 뒤 자동 숨김 (0이면 수동)
  },
  goallog: {
    show: true,
    title: "득점 기록",
    items: [],               // {team:'home'|'away', teamName, minute, scorer}
  },
  predict: {
    show: true,
    title: "스코어 예측",
    items: [
      { name: "참가자 1", home: 2, away: 1 },
      { name: "참가자 2", home: 1, away: 0 },
      { name: "참가자 3", home: 2, away: 0 },
      { name: "참가자 4", home: 0, away: 0 },
      { name: "참가자 5", home: 3, away: 1 },
      { name: "참가자 6", home: 1, away: 1 },
    ],
  },
  ticker: {
    show: true,
    label: "주요뉴스",
    mode: "auto",            // auto=구글뉴스RSS / manual=직접입력
    query: "월드컵",
    text: "'캡틴' 손흥민, 2026 북중미 월드컵 출격 준비 완료",
    headlines: [],
    display: "scroll",       // scroll=흐름 / rotate=한 헤드라인씩
    rotateSec: 6,            // rotate 모드 전환 주기(초)
    feed: [],               // 별풍선 후원이 티커에 끼어 흐르는 임시 항목 [{text,ts}]
  },
  osc: {
    enabled: false,            // 전체 ON/OFF
    host: "127.0.0.1",         // 대상 IP (공통)
    port: 9000,                // 포트 (공통)
    cues: {                    // 득점 트리거별 큐: 각 메시지를 ms 지연 후 전송
      home: [ { ms: 0, addr: "/goal/home" } ],
      away: [ { ms: 0, addr: "/goal/away" } ],
    },
  },
  api: {
    token: "",
    competition: "WC",       // football-data 대회 코드 (월드컵=WC)
    matchId: "",
    liveSync: false,
    afKey: "",               // API-Football(api-sports.io) 키 — 포메이션/라인업용
    afFixture: "",           // API-Football 경기(fixture) ID
  },
  formation: {
    show: false,
    homeFormation: "4-3-3",
    awayFormation: "4-3-3",
    homeColor: { fill: "#2552ab", text: "#ffffff" },   // 유니폼/번호 색 (API 자동 또는 수동 지정)
    awayColor: { fill: "#ab2727", text: "#ffffff" },
    // 영문→한글 이름 매핑 (라인업 불러올 때 자동 치환). 컨트롤에서 추가/수정.
    nameMap: {
      "Heung-Min Son": "손흥민", "Min-Jae Kim": "김민재", "Kang-In Lee": "이강인",
      "Hee-Chan Hwang": "황희찬", "In-Beom Hwang": "황인범", "Jae-Sung Lee": "이재성",
      "Seung-Gyu Kim": "김승규", "Hyeon-Woo Jo": "조현우", "Gue-Sung Cho": "조규성",
      "Young-Gwon Kim": "김영권", "Moon-Hwan Kim": "김문환", "Jin-Su Kim": "김진수",
      "Woo-Young Jung": "정우영", "Seung-Ho Paik": "백승호", "Hyun-Gyu Oh": "오현규",
    },
    // 선수 배열: [GK, 수비줄…, 미드줄…, 공격줄…] 순서 (포메이션 줄 합 + GK = 11)
    homePlayers: [
      { num: 1, name: "김승규" }, { num: 2, name: "김문환" }, { num: 4, name: "김민재" },
      { num: 19, name: "김영권" }, { num: 14, name: "홍철" }, { num: 6, name: "황인범" },
      { num: 16, name: "정우영" }, { num: 17, name: "이재성" }, { num: 7, name: "손흥민" },
      { num: 9, name: "조규성" }, { num: 11, name: "황희찬" },
    ],
    awayPlayers: [
      { num: 1, name: "" }, { num: 2, name: "" }, { num: 3, name: "" },
      { num: 4, name: "" }, { num: 5, name: "" }, { num: 6, name: "" },
      { num: 7, name: "" }, { num: 8, name: "" }, { num: 9, name: "" },
      { num: 10, name: "" }, { num: 11, name: "" },
    ],
  },
  sound: {                   // 득점 시 재생할 사운드(노래)
    enabled: true,
    volume: 0.8,
    home: "example-goal.wav", // public/sounds 의 파일명 또는 URL (교체 가능)
    away: "example-goal.wav",
  },
  soundEvent: { token: 0, team: "" },  // 득점/테스트 시 증가 → 오버레이가 감지해 재생
  weflab: { url: "", watching: false, frames: 0, lastAt: 0, error: null },  // 위플랩 별풍선 감시
  donation: {                // 별풍선 알림 표시
    show: false,
    enabled: true,
    template: "🎈 {donor}님 별풍선 {balloons}개 감사합니다! {message}",
    durationSec: 6,
    minBalloons: 0,          // 이 개수 이상만 표시(0=전부)
    toBanner: true,          // 가운데 배너로 표시
    toTicker: true,          // 하단 뉴스 티커에 흘려보내기
    tickerSec: 25,           // 티커에 머무는 시간(초) — 지나면 자동 제거
  },
  donationEvent: { token: 0, text: "", donor: "", balloons: 0, amount: 0, message: "" },
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

// 빌드용 로컬 프리셋(있으면): API 키 등 코드에 박아두기 — public repo에는 미포함(.gitignore)
let PRESET = null;
try { PRESET = require("./preset.local"); deepMerge(DEFAULT_STATE, PRESET); console.log("프리셋(preset.local) 적용됨"); } catch {}

let state = loadState();
// 프리셋 키는 저장된 값이 비어있으면 채워줌(빈 문자열이 덮어쓰는 것 방지)
if (PRESET && PRESET.api) for (const k of Object.keys(PRESET.api)) { if (!state.api[k]) state.api[k] = PRESET.api[k]; }
// 재시작 시 타이머는 멈춘 상태로
state.timer.running = false; state.timer.startedAt = null;
// 재시작 시 감시는 꺼진 상태(수동 시작)
if (state.weflab) { state.weflab.watching = false; }
if (state.donation) { state.donation.show = false; }
if (state.ticker) { state.ticker.feed = []; }   // 후원 피드는 재시작 시 비움

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
function sendOscMessage(host, port, addr) {
  try {
    const packet = Buffer.concat([oscString(addr), oscString(",")]); // 주소 + 빈 타입태그
    oscSock.send(packet, port, host, (e) => { if (e) console.log("OSC 전송 실패:", e.message); });
    console.log(`OSC → ${host}:${port}  ${addr}`);
  } catch (e) { console.log("OSC 오류:", e.message); }
}
// 득점 트리거 → 해당 팀 큐를 ms 타이밍에 맞춰 순서대로 전송
function sendOSCForTeam(team) {
  if (!state.osc.enabled) return;
  const host = state.osc.host || "127.0.0.1";
  const port = Number(state.osc.port) || 9000;
  const cue = (state.osc.cues && state.osc.cues[team]) || [];
  for (const item of cue) {
    if (!item || !item.addr) continue;
    const ms = Math.max(0, Number(item.ms) || 0);
    if (ms === 0) sendOscMessage(host, port, item.addr);
    else setTimeout(() => sendOscMessage(host, port, item.addr), ms);
  }
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
    // OSC (메시지만, 켜진 모든 대상으로)
    sendOSCForTeam(team);
    // 사운드 재생 트리거 (오버레이가 감지)
    state.soundEvent = { token: ((state.soundEvent && state.soundEvent.token) || 0) + 1, team };
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

// 영문→한글 이름 치환 (표기/순서 달라도 매칭되게 정규화 + 정렬키)
function normName(s) { return String(s || "").toLowerCase().replace(/[^a-z가-힣]/g, ""); }
function sortName(s) { return String(s || "").toLowerCase().replace(/[.]/g, "").split(/[\s\-]+/).filter(Boolean).sort().join(""); }
function applyNameMap(players) {
  const map = state.formation.nameMap || {};
  const byNorm = {}, bySort = {};
  for (const k of Object.keys(map)) { if (map[k]) { byNorm[normName(k)] = map[k]; bySort[sortName(k)] = map[k]; } }
  return (players || []).map((p) => {
    const ko = byNorm[normName(p.name)] || bySort[sortName(p.name)];
    return ko ? { ...p, name: ko } : p;
  });
}

// ── API-Football (포메이션/라인업) ───────────────────────────
async function afFetch(pathQuery) {
  if (!state.api.afKey) throw new Error("API-Football 키가 없습니다 (컨트롤에서 입력).");
  const r = await fetch(`https://v3.football.api-sports.io${pathQuery}`, { headers: { "x-apisports-key": state.api.afKey } });
  if (!r.ok) throw new Error(`API-Football ${r.status}`);
  return r.json();
}
// 경기 검색 → fixture ID 목록 (리그/시즌 또는 날짜)
async function findFixtures({ league, season, date }) {
  const p = new URLSearchParams();
  if (league) p.set("league", String(league));
  if (season) p.set("season", String(season));
  if (date) p.set("date", String(date));
  if (![...p.keys()].length) throw new Error("리그+시즌 또는 날짜를 입력하세요.");
  const data = await afFetch(`/fixtures?${p.toString()}`);
  return (data.response || []).map((f) => ({
    id: f.fixture?.id,
    date: f.fixture?.date,
    status: f.fixture?.status?.short,
    round: f.league?.round,
    home: f.teams?.home?.name,
    away: f.teams?.away?.name,
  }));
}
async function loadLineups() {
  if (!state.api.afKey) throw new Error("API-Football 키가 없습니다 (컨트롤에서 입력).");
  if (!state.api.afFixture) throw new Error("fixture ID가 없습니다.");
  const r = await fetch(`https://v3.football.api-sports.io/fixtures/lineups?fixture=${encodeURIComponent(state.api.afFixture)}`, {
    headers: { "x-apisports-key": state.api.afKey },
  });
  if (!r.ok) throw new Error(`API-Football ${r.status}`);
  const data = await r.json();
  const arr = data.response || [];
  if (!arr.length) throw new Error("라인업이 아직 없습니다 — 보통 킥오프 ~1시간 전부터 제공됩니다.");
  const toPlayers = (xi) => (xi || []).map((p) => ({
    num: p.player?.number ?? "", name: p.player?.name || "", grid: p.player?.grid || "", pos: p.player?.pos || "",
  }));
  const toColor = (t) => { const c = t.team?.colors?.player; return c && c.primary ? { fill: "#" + c.primary, text: "#" + (c.number || "ffffff") } : null; };
  const home = arr[0], away = arr[1] || {};
  if (home.formation) state.formation.homeFormation = home.formation;
  if (home.startXI) state.formation.homePlayers = applyNameMap(toPlayers(home.startXI));
  if (away.formation) state.formation.awayFormation = away.formation;
  if (away.startXI) state.formation.awayPlayers = applyNameMap(toPlayers(away.startXI));
  const hc = toColor(home); if (hc) state.formation.homeColor = hc;
  const ac = toColor(away); if (ac) state.formation.awayColor = ac;
  state.formation.show = true;
  saveState(state); broadcast();
  return { home: home.team?.name, away: away.team?.name, homeFormation: home.formation, awayFormation: away.formation };
}

// ── 별풍선 알림 ──────────────────────────────────────────────
let donationHideTimer = null;
function fireDonation(ev) {
  if (!state.donation.enabled) return;
  const balloons = Number(ev.balloons) || 0;
  if (balloons < (Number(state.donation.minBalloons) || 0)) return;   // 임계값 미만 무시
  const text = String(state.donation.template || "")
    .replace(/\{donor\}/g, ev.donor || "익명")
    .replace(/\{balloons\}/g, balloons)
    .replace(/\{amount\}/g, Number(ev.amount) || 0)
    .replace(/\{message\}/g, ev.message || "")
    .replace(/\s+/g, " ").trim();

  // 가운데 배너 표시
  if (state.donation.toBanner !== false) {
    state.donationEvent = {
      token: ((state.donationEvent && state.donationEvent.token) || 0) + 1,
      text, donor: ev.donor || "", balloons, amount: Number(ev.amount) || 0, message: ev.message || "",
    };
    state.donation.show = true;
    if (donationHideTimer) clearTimeout(donationHideTimer);
    const dur = Number(state.donation.durationSec) || 6;
    if (dur > 0) donationHideTimer = setTimeout(() => { state.donation.show = false; saveState(state); broadcast(); }, dur * 1000);
  }
  // 하단 뉴스 티커에 흘려보내기 (최근 6건 유지, tickerSec 후 자동 제거)
  if (state.donation.toTicker !== false) {
    state.ticker.feed = [{ text, ts: Date.now() }, ...(state.ticker.feed || [])].slice(0, 6);
    const ttl = (Number(state.donation.tickerSec) || 25) * 1000;
    setTimeout(pruneFeed, ttl + 200);
  }
  saveState(state); broadcast();
}
// 오래된 후원 피드 항목 제거(자동 사라짐) — 바뀌면 브로드캐스트
function pruneFeed() {
  const ttl = (Number(state.donation && state.donation.tickerSec) || 25) * 1000;
  const now = Date.now();
  const feed = (state.ticker && state.ticker.feed) || [];
  const kept = feed.filter((f) => f && f.ts && (now - f.ts) < ttl);
  if (kept.length !== feed.length) { state.ticker.feed = kept; saveState(state); broadcast(); }
}
setInterval(pruneFeed, 5000);  // 백업: 5초마다 만료분 정리
// 위플랩 감시기 이벤트 연결
if (watcher) {
  watcher.onEvent((ev) => fireDonation(ev));
  watcher.onStatus((st) => { state.weflab.watching = !!st.watching; state.weflab.frames = st.frames || 0; state.weflab.error = st.error || null; broadcast(); });
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

    // 사운드 파일 목록 (동봉 + 업로드 폴더 합침)
    if (url === "/sounds") {
      const set = new Set();
      for (const d of [SOUNDS_BUNDLED, SOUNDS_UPLOAD]) {
        try { fs.readdirSync(d).forEach((f) => { if (/\.(mp3|wav|ogg|m4a|aac)$/i.test(f)) set.add(f); }); } catch {}
      }
      return json(res, 200, { files: [...set] });
    }
    // 사운드 파일 업로드 → 쓰기 가능한 업로드 폴더에 저장
    if (url === "/sounds/upload" && req.method === "POST") {
      const qm = (req.url.split("?")[1] || "").match(/(?:^|&)name=([^&]*)/);
      const raw = qm ? decodeURIComponent(qm[1]) : "upload.bin";
      const safe = raw.replace(/[\/\\]/g, "").replace(/[^\w.\-가-힣() ]/g, "_").replace(/\.{2,}/g, ".");
      if (!/\.(mp3|wav|ogg|m4a|aac)$/i.test(safe)) return json(res, 200, { ok: false, error: "오디오 파일(mp3/wav/ogg/m4a)만 가능합니다." });
      const chunks = []; let size = 0, tooBig = false;
      req.on("data", (c) => { size += c.length; if (size > 40 * 1024 * 1024) tooBig = true; if (!tooBig) chunks.push(c); });
      req.on("end", () => {
        if (tooBig) return json(res, 200, { ok: false, error: "파일이 너무 큽니다 (40MB 초과)." });
        try {
          fs.mkdirSync(SOUNDS_UPLOAD, { recursive: true });
          fs.writeFileSync(path.join(SOUNDS_UPLOAD, safe), Buffer.concat(chunks));
          console.log(`사운드 업로드: ${safe} (${size} bytes)`);
          return json(res, 200, { ok: true, file: safe });
        } catch (e) { return json(res, 200, { ok: false, error: e.message }); }
      });
      return;
    }
    // 사운드 파일 제공 (업로드 폴더 우선, 없으면 동봉)
    if (url.startsWith("/sounds/")) {
      const f = decodeURIComponent(url.slice("/sounds/".length));
      if (f.includes("..") || f.includes("/")) return res.writeHead(403).end("forbidden");
      try {
        let fp = path.join(SOUNDS_UPLOAD, f);
        if (!fs.existsSync(fp)) fp = path.join(SOUNDS_BUNDLED, f);
        const data = fs.readFileSync(fp);
        const ext = (f.split(".").pop() || "").toLowerCase();
        const mime = { mp3: "audio/mpeg", wav: "audio/wav", ogg: "audio/ogg", m4a: "audio/mp4", aac: "audio/aac" }[ext] || "application/octet-stream";
        res.writeHead(200, { "Content-Type": mime, ...NOCACHE });
        return res.end(data);
      } catch { return res.writeHead(404).end("not found"); }
    }

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

    // 사운드 테스트 재생 (점수/기록 변화 없이 오버레이에서만 재생)
    if (url === "/sound/test" && req.method === "POST") {
      const { team } = JSON.parse((await readBody(req)) || "{}");
      state.soundEvent = { token: ((state.soundEvent && state.soundEvent.token) || 0) + 1, team: team === "away" ? "away" : "home" };
      broadcast();
      return json(res, 200, { ok: true });
    }

    if (url === "/timer" && req.method === "POST") {
      const { cmd, half, sec } = JSON.parse((await readBody(req)) || "{}");
      const t = state.timer;
      if (cmd === "start" && !t.running) { t.startedAt = Date.now(); t.running = true; }
      else if (cmd === "pause" && t.running) { t.elapsedSec = currentSec(); t.running = false; t.startedAt = null; }
      else if (cmd === "reset") { t.elapsedSec = 0; t.running = false; t.startedAt = null; }
      else if (cmd === "half") { if (half === 1 || half === 2) t.half = half; }
      else if (cmd === "set") { t.elapsedSec = Math.max(0, Number(sec) || 0); if (t.running) t.startedAt = Date.now(); } // 직접 시간 설정
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
    // 경기 검색 (fixture ID 찾기)
    if (url === "/api/fixtures" && req.method === "POST") {
      const body = JSON.parse((await readBody(req)) || "{}");
      if (body.afKey != null) state.api.afKey = body.afKey;
      saveState(state);
      try { const fixtures = await findFixtures(body); return json(res, 200, { ok: true, fixtures }); }
      catch (e) { return json(res, 200, { ok: false, error: e.message }); }
    }
    // 포메이션/라인업 불러오기 (API-Football)
    if (url === "/api/lineups" && req.method === "POST") {
      const body = JSON.parse((await readBody(req)) || "{}");
      if (body.afKey != null) state.api.afKey = body.afKey;
      if (body.afFixture != null) state.api.afFixture = body.afFixture;
      saveState(state);
      try { const r = await loadLineups(); return json(res, 200, { ok: true, info: r }); }
      catch (e) { return json(res, 200, { ok: false, error: e.message }); }
    }

    // 이름 한글 매핑 적용 (매핑 저장 + 현재 선수에 즉시 적용)
    if (url === "/formation/applynames" && req.method === "POST") {
      const body = JSON.parse((await readBody(req)) || "{}");
      if (body.nameMap && typeof body.nameMap === "object") state.formation.nameMap = body.nameMap;
      state.formation.homePlayers = applyNameMap(state.formation.homePlayers);
      state.formation.awayPlayers = applyNameMap(state.formation.awayPlayers);
      saveState(state); broadcast();
      return json(res, 200, { ok: true });
    }

    // 위플랩 별풍선 감시 시작/정지/테스트
    if (url === "/weflab/start" && req.method === "POST") {
      const { url: wurl } = JSON.parse((await readBody(req)) || "{}");
      if (wurl != null) state.weflab.url = wurl;
      if (!watcher) return json(res, 200, { ok: false, error: "puppeteer 미설치 — 폴더에서 'npm install' 후 다시 시작하세요." });
      try { await watcher.start(state.weflab.url); state.weflab.watching = true; saveState(state); broadcast(); return json(res, 200, { ok: true }); }
      catch (e) { state.weflab.error = e.message; broadcast(); return json(res, 200, { ok: false, error: e.message }); }
    }
    if (url === "/weflab/stop" && req.method === "POST") {
      if (watcher) { try { await watcher.stop(); } catch {} }
      state.weflab.watching = false; saveState(state); broadcast();
      return json(res, 200, { ok: true });
    }
    if (url === "/weflab/test" && req.method === "POST") {
      const b = JSON.parse((await readBody(req)) || "{}");
      fireDonation({ donor: b.donor || "테스트후원자", balloons: Number(b.balloons) || 0, amount: Number(b.amount) || 0, message: b.message || "테스트 메시지" });
      return json(res, 200, { ok: true });
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
  return {
    ...s,
    _clock: { sec, text: `${label} ${mm}:${ss}`, running: s.timer.running, startedAt: s.timer.startedAt, elapsedSec: s.timer.elapsedSec },
    _server: { lan: LAN_IP, port: activePort },   // 다른 PC 접속 주소 안내용
  };
}

// 라이브 동기화 + 뉴스 주기 갱신
setInterval(() => { if (state.api.liveSync && state.api.matchId) syncMatch().catch(() => {}); }, 20000);
setInterval(() => refreshNews().catch(() => {}), 5 * 60 * 1000);

// 0.0.0.0 → 같은 네트워크의 다른 PC에서도 접속 가능
server.on("listening", () => {
  const port = activePort;
  console.log(`\n  ⚽  wc-overlay 실행 중`);
  console.log(`  ──────────────────────────────────────────────────`);
  console.log(`  [이 맥에서]`);
  console.log(`    컨트롤 페이지 :  http://localhost:${port}/control`);
  console.log(`    오버레이      :  http://localhost:${port}/`);
  console.log(`  [다른 컴퓨터(같은 와이파이/공유기)에서 — vMix PC 등]`);
  console.log(`    오버레이      :  http://${LAN_IP}:${port}/`);
  console.log(`    컨트롤        :  http://${LAN_IP}:${port}/control`);
  console.log(`  ──────────────────────────────────────────────────`);
  console.log(`  ※ 다른 PC에서 안 열리면 맥 '시스템 설정 > 네트워크 > 방화벽'에서`);
  console.log(`     node 들어오는 연결 허용 (또는 방화벽 잠시 끄기).`);
  console.log(`  ──────────────────────────────────────────────────\n`);
  refreshNews().catch(() => {});
});
function startListening(port) {
  activePort = port;
  server.listen(port, "0.0.0.0");
}
server.on("error", (e) => {
  if (e.code === "EADDRINUSE") {
    const used = activePort;
    if (used - (Number(PORT) || 8093) < 10) {
      console.log(`  ⚠  포트 ${used} 가 이미 사용 중 → ${used + 1} 로 다시 시도합니다…`);
      setTimeout(() => startListening(used + 1, 0), 200);
    } else {
      console.log(`\n  ✖  사용 가능한 포트를 찾지 못했습니다 (${PORT}~${used}).`);
      console.log(`     이미 wc-overlay가 켜져 있을 수 있어요. 기존 창을 닫거나,`);
      console.log(`     다른 포트로 실행: PORT=9100 node server.js\n`);
      process.exit(1);
    }
  } else {
    console.log("서버 오류:", e.message);
    process.exit(1);
  }
});
startListening(Number(PORT) || 8093);
