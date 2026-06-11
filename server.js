// ============================================================
//  wc-overlay : 월드컵 중계 스타일 vMix 오버레이 (SBS 16강 디자인)
//   - http://localhost:8093/          → 오버레이 (vMix 브라우저 소스)
//   - http://localhost:8093/control   → 컨트롤 페이지
//   - http://localhost:8093/events    → 상태 실시간 푸시 (SSE)
//   - POST /update                    → 컨트롤이 상태 변경
//  의존성 없음. 실행: node server.js
// ============================================================
const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = process.env.PORT || 8093;
const DIR = __dirname;
const STATE_FILE = path.join(DIR, ".state.json");

// ── 기본 상태 (이미지 디자인 기준 초기값) ───────────────────
const DEFAULT_STATE = {
  scoreboard: {
    show: true,
    title: "2022 카타르 월드컵™",
    round: "16강",
    homeName: "대한민국",
    homeFlag: "🇰🇷",
    homeScore: 2,
    awayName: "포르투갈",
    awayFlag: "🇵🇹",
    awayScore: 1,
    clock: "후반 36:42",
  },
  banner: {
    show: true,
    line1: "16강 진출 확정!",
    line2: "대한민국",
    flag: "🇰🇷",
  },
  goal: {
    show: true,
    minute: "37'",
    headline: "골! 대한민국",
    player: "7. 손흥민",
  },
  crew: {
    show: true,
    caster: "배성재",
    casterRole: "캐스터",
    analyst: "박지성",
    analystRole: "해설위원",
  },
  next: {
    show: true,
    label: "다음경기",
    datetime: "12/6 (화) 00:00",
    homeName: "브라질",
    homeFlag: "🇧🇷",
    awayName: "일본",
    awayFlag: "🇯🇵",
  },
  ticker: {
    show: true,
    label: "주요뉴스",
    text: "'캡틴' 손흥민, A매치 통산 35번째 골! 황선홍 넘고 역대 최다 2위",
  },
};

function loadState() {
  try {
    const saved = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    return deepMerge(structuredClone(DEFAULT_STATE), saved);
  } catch {
    return structuredClone(DEFAULT_STATE);
  }
}
function saveState(s) {
  try { fs.writeFileSync(STATE_FILE, JSON.stringify(s, null, 2)); } catch {}
}
function deepMerge(base, patch) {
  for (const k of Object.keys(patch || {})) {
    if (patch[k] && typeof patch[k] === "object" && !Array.isArray(patch[k])) {
      base[k] = deepMerge(base[k] || {}, patch[k]);
    } else {
      base[k] = patch[k];
    }
  }
  return base;
}

let state = loadState();
const clients = new Set(); // SSE 연결들

function broadcast() {
  const payload = `data: ${JSON.stringify(state)}\n\n`;
  for (const res of clients) {
    try { res.write(payload); } catch {}
  }
}

const NOCACHE = {
  "Cache-Control": "no-store, no-cache, must-revalidate",
  Pragma: "no-cache",
  Expires: "0",
};

function serveFile(res, file, type) {
  try {
    res.writeHead(200, { "Content-Type": type, ...NOCACHE });
    res.end(fs.readFileSync(path.join(DIR, "public", file)));
  } catch {
    res.writeHead(404).end("not found");
  }
}

const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0];

  if (url === "/" || url === "/overlay" || url === "/overlay.html") {
    return serveFile(res, "overlay.html", "text/html; charset=utf-8");
  }
  if (url === "/control" || url === "/control.html") {
    return serveFile(res, "control.html", "text/html; charset=utf-8");
  }

  // 현재 상태 (오버레이/컨트롤 최초 로드용)
  if (url === "/state") {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8", ...NOCACHE });
    return res.end(JSON.stringify(state));
  }

  // SSE 스트림
  if (url === "/events") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      ...NOCACHE,
    });
    res.write(`data: ${JSON.stringify(state)}\n\n`);
    clients.add(res);
    const ping = setInterval(() => { try { res.write(": ping\n\n"); } catch {} }, 15000);
    req.on("close", () => { clearInterval(ping); clients.delete(res); });
    return;
  }

  // 상태 업데이트 (부분 패치)
  if (url === "/update" && req.method === "POST") {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      try {
        const patch = JSON.parse(body || "{}");
        if (patch.__reset) {
          state = structuredClone(DEFAULT_STATE);
        } else {
          state = deepMerge(state, patch);
        }
        saveState(state);
        broadcast();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, state }));
      } catch (e) {
        res.writeHead(400).end(JSON.stringify({ ok: false, error: String(e) }));
      }
    });
    return;
  }

  res.writeHead(404).end("not found");
});

server.listen(PORT, () => {
  console.log(`\n  ⚽  wc-overlay 실행 중`);
  console.log(`  ──────────────────────────────────────────`);
  console.log(`  오버레이 (vMix 브라우저 소스):  http://localhost:${PORT}/`);
  console.log(`  컨트롤 페이지:                  http://localhost:${PORT}/control`);
  console.log(`  ──────────────────────────────────────────\n`);
});
