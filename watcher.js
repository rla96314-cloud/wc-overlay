// ============================================================
//  위플랩 후원/별풍선 추출기 (CDP 웹소켓 프레임 캡처 방식)
//   - weflab 알림(alert)/후원 오버레이 페이지를 헤드리스로 띄우고,
//     socket.io 웹소켓 프레임(42[...])을 CDP로 가로채 후원 이벤트를 파싱.
//   - 별풍선 갯수 / 후원 금액(원) / 후원자 닉네임 / 메시지를 추출 → onEvent.
//   - 키 이름을 정확히 모를 때를 대비해 원본 프레임도 onRawFrame 으로 흘려보냄
//     (패널의 "원본 프레임" 탭에서 보고 키 매핑을 튜닝).
//   - idle 동안 이벤트 없으면 페이지 자동 새로고침(stale 연결 방지).
// ============================================================
const puppeteer = require("puppeteer");

let browser = null;
let page = null;
let onEventCb = () => {};
let onRawCb = () => {};
let onStatusCb = () => {};
let onAlertCb = () => {};

const state = { url: null, watching: false, last: null, lastAt: 0, error: null, frames: 0, alertPhase: null, alertPhaseAt: 0 };

let lastActivityAt = 0;
let watchdogTimer = null;
let staleTimer = null;
let restarting = false;
const IDLE_RESTART_MS = 90000; // 이 시간 동안 프레임(후원/keepalive) 없으면 새로고침
const DEBUG = process.env.WEFLAB_DEBUG === "1";

// ── 후원 이벤트 키 매핑 (패널에서 덮어쓸 수 있음) ───────────
let keymap = {
  // 이 이벤트 이름들이 들어오면 후원으로 간주 (비어있으면 금액/별풍선 필드로 추정)
  eventNames: [
    "sendballoon", "balloon", "donation", "donate", "gift",
    "cheese", "cash", "sponsor", "msg", "message", "alarm", "alert",
  ],
  balloonKeys: ["balloon", "ballon", "ballooncnt", "ballooncount", "cnt", "count", "num", "gift", "cheese", "quantity", "qty", "ea"],
  amountKeys: ["amount", "price", "cash", "won", "krw", "money", "totalprice", "total", "pay", "value"],
  donorKeys: ["nickname", "nick", "usernick", "username", "from", "sender", "name", "user", "id", "donator"],
  msgKeys: ["message", "msg", "comment", "text", "content", "memo"],
  balloonToWon: 0, // >0 이면 별풍선×이 값으로 금액(원)도 함께 채움 (아프리카 별풍선 1개≈100원)
  // data.type 이 이 중 하나면 value 를 "별풍선 갯수"로, 아니면 "금액(원)"으로 해석
  balloonTypes: ["SENDBALLOON", "BALLOON", "별풍선", "star", "ADBALLOON"],
  alertSelector: ".page_area.alert", // 후원 알림 컨테이너 (알림 끝나면 OFF 감지용)
  alertActiveSelector: ".donation_wrap", // 이 안의 요소에 'on' 클래스가 있으면 알림 표시 중
};
function setKeymap(partial) {
  keymap = { ...keymap, ...(partial || {}) };
}

// ── 프레임 파싱 ───────────────────────────────────────────
function lc(s) { return String(s || "").toLowerCase(); }

// 객체(중첩 포함)에서 키 목록에 맞는 첫 숫자/문자 값 찾기
function findByKeys(obj, keys, want /* "num" | "str" */, depth = 0) {
  if (!obj || typeof obj !== "object" || depth > 4) return null;
  const lkeys = keys.map(lc);
  // 1차: 현재 레벨 직접 매칭
  for (const k of Object.keys(obj)) {
    if (lkeysIncludes(lkeys, lc(k))) {
      const v = obj[k];
      if (want === "num") { const n = toNum(v); if (n != null) return n; }
      else if (typeof v === "string" && v.trim()) return v.trim();
      else if (v != null && typeof v !== "object") return String(v);
    }
  }
  // 2차: 중첩 객체 재귀
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    if (v && typeof v === "object") {
      const r = findByKeys(v, keys, want, depth + 1);
      if (r != null) return r;
    }
  }
  return null;
}
function lkeysIncludes(lkeys, key) {
  for (const lk of lkeys) if (key === lk || key.includes(lk)) return true;
  return false;
}
function toNum(v) {
  if (typeof v === "number" && isFinite(v)) return v;
  if (typeof v === "string") {
    const cleaned = v.replace(/[, ]/g, "").replace(/원|개|won|krw/gi, "");
    const n = Number(cleaned);
    if (isFinite(n) && cleaned !== "") return n;
  }
  return null;
}

// socket.io 텍스트 프레임 → { event, payload } | null
function parseSocketIO(data) {
  if (typeof data !== "string") return null;
  const i = data.indexOf("[");
  if (i < 0) return null;
  const prefix = data.slice(0, i);
  // 42 / 42/ns, ... 형태만 (이벤트 메시지). 0,3,40 등 핸드셰이크는 제외
  if (!/^4[0-9]/.test(prefix)) return null;
  try {
    const arr = JSON.parse(data.slice(i));
    if (!Array.isArray(arr) || arr.length === 0) return null;
    const event = typeof arr[0] === "string" ? arr[0] : "";
    const payload = arr.length > 2 ? arr.slice(1) : arr[1];
    return { event, payload };
  } catch (e) {
    return null;
  }
}

// 진단용: payload(중첩 포함)에서 숫자/문자 필드를 key=value 로 수집 (최대 14개)
function collectFields(obj, prefix = "", out = { nums: [], strs: [] }, depth = 0) {
  if (!obj || typeof obj !== "object" || depth > 3) return out;
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    const path = prefix ? prefix + "." + k : k;
    if (typeof v === "number" && isFinite(v)) { if (out.nums.length < 14) out.nums.push(path + "=" + v); }
    else if (typeof v === "string" && v.trim() && v.length < 40) { if (out.strs.length < 8) out.strs.push(path + '="' + v.trim() + '"'); }
    else if (v && typeof v === "object") collectFields(v, path, out, depth + 1);
  }
  return out;
}

// 파싱된 프레임이 후원 이벤트인지 판단 + 구조화
function extractDonation(parsed) {
  const { event, payload } = parsed;

  // ── weflab 릴레이 표준 형태 우선 처리 ──────────────────
  //  ["msg", { type:"donation"/"test_donation", data:{ platform, type, value, uname, uid, msg } }]
  //  body.type 에 "donation" 이 있을 때만 후원으로 인정(채팅/시스템 msg 는 제외),
  //  data.type 으로 별풍선(갯수) vs 금액(원) 을 구분한다.
  const body = Array.isArray(payload) ? payload[0] : payload;
  if (body && typeof body === "object" && body.data && typeof body.data === "object") {
    // weflab 릴레이 프레임 — 후원이 아니면(채팅 등) 여기서 무시
    if (!lc(body.type || "").includes("donation")) return null;
    const d = body.data;
    const dType = String(d.type || "");
    const val = toNum(d.value) || 0;
    const isBalloon = keymap.balloonTypes.some((t) => lc(dType).includes(lc(t)));
    let balloons = 0, amount = 0;
    if (isBalloon) { balloons = val; if (keymap.balloonToWon > 0) amount = val * keymap.balloonToWon; }
    else { amount = val; }
    return {
      ts: Date.now(),
      event: dType || body.type || event || "donation",
      donor: String(d.uname || d.uid || d.nickname || "").trim(),
      balloons, amount,
      message: String(d.msg || d.message || "").trim(),
    };
  }

  // ── 그 외 형태: 키 이름 휴리스틱 (fallback) ───────────
  const evLc = lc(event);
  const nameMatch = keymap.eventNames.some((n) => evLc.includes(lc(n)));

  const balloons = findByKeys(payload, keymap.balloonKeys, "num");
  let amount = findByKeys(payload, keymap.amountKeys, "num");
  const donor = findByKeys(payload, keymap.donorKeys, "str");
  const message = findByKeys(payload, keymap.msgKeys, "str");

  // 후원으로 인정: 이벤트 이름이 매칭되거나, 별풍선/금액 숫자가 잡힌 경우
  const hasValue = balloons != null || amount != null;
  if (!nameMatch && !hasValue) return null;

  if (amount == null && balloons != null && keymap.balloonToWon > 0) {
    amount = balloons * keymap.balloonToWon;
  }

  return {
    ts: Date.now(),
    event: event || "(unnamed)",
    donor: donor || "",
    balloons: balloons != null ? balloons : 0,
    amount: amount != null ? amount : 0,
    message: message || "",
  };
}

function emitEvent(ev) {
  state.last = ev;
  state.lastAt = Date.now();
  lastActivityAt = Date.now();
  console.log(`[후원] ${ev.donor || "?"} | 별풍선 ${ev.balloons} | ${ev.amount}원 | "${ev.message}"`);
  onEventCb(ev);
}

async function ensureBrowser() {
  if (!browser || !browser.connected) {
    browser = await puppeteer.launch({
      headless: "new",
      args: [
        "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
        "--autoplay-policy=no-user-gesture-required",
        "--disable-web-security",
        "--disable-background-timer-throttling",
        "--disable-renderer-backgrounding",
        "--disable-backgrounding-occluded-windows",
        "--mute-audio",
        "--disable-gpu",
      ],
    });
  }
  return browser;
}

async function start(url) {
  if (!url) throw new Error("URL 비어있음");
  await stop();
  await ensureBrowser();
  state.error = null;
  state.last = null;
  state.frames = 0;

  page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 720 });
  page.on("pageerror", (e) => DEBUG && console.log("[DBG pageerror]", e.message));

  // 후원 알림이 떴다 사라지는 것을 감지 → "알림 끝나면 OFF" 규칙용
  await page.exposeFunction("__weflabOnAlert", (phase) => {
    state.alertPhase = phase;
    state.alertPhaseAt = Date.now();
    console.log(`[알림] ${phase === "start" ? "표시 시작" : "종료 감지 → OFF 신호"}`);
    onAlertCb(phase);
  }).catch(() => {});

  // CDP: socket.io 웹소켓 프레임 캡처 (송·수신 모두)
  try {
    const cdp = await page.target().createCDPSession();
    await cdp.send("Network.enable").catch(() => {});
    const handleFrame = (payloadData) => {
      const d = String(payloadData || "");
      if (!/^4[0-9]/.test(d)) return; // 이벤트 메시지 프레임만
      state.frames++;
      lastActivityAt = Date.now();
      const parsed = parseSocketIO(d);
      if (!parsed) return;
      // 원본 프레임 패널로 (learn 모드 튜닝용)
      onRawCb({ ts: Date.now(), event: parsed.event, raw: d.slice(0, 600) });
      const ev = extractDonation(parsed);
      if (ev) {
        // 진단: 어떤 키를 별풍선/금액으로 골랐는지 + 후보 숫자필드
        const f = collectFields(parsed.payload);
        onRawCb({
          ts: Date.now(), event: "파싱결과",
          raw: `별풍선=${ev.balloons} 금액=${ev.amount}원 (이벤트="${ev.event}") | 숫자필드: ${f.nums.join(", ") || "없음"} | 문자필드: ${f.strs.join(", ") || "없음"}`,
        });
        emitEvent(ev);
      }
    };
    cdp.on("Network.webSocketFrameReceived", (e) => handleFrame(e.response && e.response.payloadData));
    cdp.on("Network.webSocketFrameSent", (e) => DEBUG && handleFrame(e.response && e.response.payloadData));
    // weflab 소켓이 닫히면(유휴 중 stale) 즉시 재연결 확인 → 쏴도 못 받는 상황 방지
    cdp.on("Network.webSocketClosed", () => { console.log("[연결] weflab 소켓 닫힘 감지"); scheduleStaleReload(); });
  } catch (e) { DEBUG && console.log("[DBG cdp]", e.message); }

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
  } catch (e) { state.error = e.message; }

  await injectAlertObserver();

  state.url = url;
  state.watching = true;
  lastActivityAt = Date.now();
  startWatchdog();
  onStatusCb(status());
  console.log(`[감시 시작] ${url}`);
  return status();
}

// 페이지에 주입: 후원 알림 컨테이너가 채워졌다(=알림 시작) 비워질 때(=알림 종료) 신호
function pageAlertObserver(cfg) {
  if (window.__weflabAlertActive) { window.__weflabAlertSel = cfg.sel; window.__weflabActiveSel = cfg.activeSel; return "dup"; }
  window.__weflabAlertActive = true;
  window.__weflabAlertSel = cfg.sel;
  window.__weflabActiveSel = cfg.activeSel;
  const settle = cfg.settleMs || 400;
  // animate.css 의 "사라지는" 애니메이션 클래스들 (이게 붙으면 알림이 닫히는 중)
  const OUT = /(fadeout|zoomout|slideout|bounceout|flipout|backout|rotateout|lightspeedout|rollout|hinge)/i;
  let active = false, endTimer = null;
  function root() { return document.querySelector(window.__weflabAlertSel) || null; }
  function shown() {
    const r = root();
    if (!r) return false;
    // weflab 알림 본체(.donation_wrap)는 표시될 때 'on' 클래스가 붙고,
    // 닫힐 때 fadeOut 등 out-애니메이션이 붙는다(요소는 DOM에 남아 텍스트만으론 판정 불가).
    const w = r.querySelector(window.__weflabActiveSel || ".donation_wrap");
    if (w) {
      const cls = " " + (w.className || "") + " ";
      if (!/ on /.test(cls)) return false;   // 'on' 없으면 알림 표시 중 아님
      if (OUT.test(cls)) return false;        // 사라지는 애니메이션 중 → 종료로 간주
      return true;
    }
    // .donation_wrap 이 없는 다른 페이지: 텍스트 유무로 폴백
    return (r.innerText || "").trim().length > 0;
  }
  function check() {
    const now = shown();
    if (now && !active) {
      active = true; clearTimeout(endTimer);
      if (window.__weflabOnAlert) window.__weflabOnAlert("start");
    } else if (!now && active) {
      clearTimeout(endTimer);
      endTimer = setTimeout(() => {
        if (!shown()) { active = false; if (window.__weflabOnAlert) window.__weflabOnAlert("end"); }
      }, settle);
    }
  }
  const observer = new MutationObserver(check);
  // 클래스(attribute) 변화까지 감시해야 on↔fadeOut 토글을 잡는다.
  observer.observe(document.body, {
    childList: true, subtree: true, characterData: true,
    attributes: true, attributeFilter: ["class"],
  });
  check();
  return "ok:" + cfg.sel;
}

async function injectAlertObserver() {
  if (!page) return;
  try {
    const r = await page.evaluate(pageAlertObserver, {
      sel: keymap.alertSelector || ".page_area.alert",
      activeSel: keymap.alertActiveSelector || ".donation_wrap",
      settleMs: 400,
    });
    console.log("[알림 감지기]", r);
  } catch (e) { DEBUG && console.log("[알림 감지기 실패]", e.message); }
}

// 페이지 새로고침으로 weflab 소켓 재연결 (워치독·소켓닫힘 공용)
async function reloadPage(reason) {
  if (restarting || !page || !state.watching) return;
  restarting = true;
  console.log(`[재연결] ${reason} → 페이지 새로고침`);
  try {
    await page.reload({ waitUntil: "domcontentloaded", timeout: 30000 });
    await injectAlertObserver();
  } catch (e) { console.warn("[재연결 실패]", e.message); }
  lastActivityAt = Date.now();
  restarting = false;
}

// 소켓이 닫히면 잠깐 기다렸다(자체 재연결 여지) 프레임이 안 오면 새로고침
function scheduleStaleReload() {
  if (staleTimer || restarting) return;
  const closedAt = Date.now();
  staleTimer = setTimeout(async () => {
    staleTimer = null;
    if (!state.watching || lastActivityAt > closedAt) return; // 새 프레임 들어왔으면 자체 복구된 것
    await reloadPage("소켓 닫힘 후 무응답");
  }, 5000);
}

function startWatchdog() {
  stopWatchdog();
  const every = Math.max(5000, Math.floor(IDLE_RESTART_MS / 3));
  watchdogTimer = setInterval(async () => {
    if (!state.watching || !page || restarting) return;
    if (Date.now() - lastActivityAt >= IDLE_RESTART_MS) {
      await reloadPage(`${Math.round((Date.now() - lastActivityAt) / 1000)}초간 프레임 없음`);
    }
  }, every);
}
function stopWatchdog() { if (watchdogTimer) { clearInterval(watchdogTimer); watchdogTimer = null; } if (staleTimer) { clearTimeout(staleTimer); staleTimer = null; } }

async function stop() {
  state.watching = false;
  stopWatchdog();
  if (page) { try { await page.close(); } catch {} page = null; }
  onStatusCb(status());
  return status();
}

function status() {
  const { url, watching, last, error, frames, alertPhase, alertPhaseAt } = state;
  return { url, watching, last, error, frames, alertPhase, alertPhaseAt };
}

// 테스트용: 가짜 후원 이벤트 한 번 발생
function inject(ev) {
  emitEvent({
    ts: Date.now(),
    event: "TEST",
    donor: ev.donor || "테스트후원자",
    balloons: Number(ev.balloons) || 0,
    amount: Number(ev.amount) || 0,
    message: ev.message || "테스트 메시지",
  });
}

// 진단: 헤드리스 페이지에서 알림 컨테이너 후보들의 현재 상태를 직접 조회
async function probe() {
  if (!page) return { error: "감시 중 아님" };
  try {
    return await page.evaluate(() => {
      const cands = [".page_area.alert", ".page_area", ".alert", "#alert", ".alert_wrap", ".alert_area", ".donation", ".widget"];
      const out = {};
      for (const sel of cands) {
        const el = document.querySelector(sel);
        if (el) out[sel] = { kids: el.querySelectorAll("*").length, textLen: (el.innerText || "").trim().length, html: el.innerHTML.slice(0, 300) };
      }
      // body 직계 자식 클래스들(알림이 어디 붙는지 단서)
      const bodyKids = [...document.body.children].map((c) => c.tagName + "." + (c.className || "").toString().slice(0, 40));
      return { title: document.title, bodyKids, candidates: out };
    });
  } catch (e) { return { error: e.message }; }
}

function onEvent(cb) { onEventCb = cb; }
function onRaw(cb) { onRawCb = cb; }
function onStatus(cb) { onStatusCb = cb; }
function onAlert(cb) { onAlertCb = cb; }

module.exports = { start, stop, status, onEvent, onRaw, onStatus, onAlert, setKeymap, inject, parseSocketIO, extractDonation, probe };
