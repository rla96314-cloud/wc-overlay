// ============================================================
//  Electron 메인 — wc-overlay 데스크톱 앱(EXE)
//   - server.js 를 자식 프로세스로 실행(ELECTRON_RUN_AS_NODE=1 → Node 설치 불필요)
//   - 컨트롤 페이지를 창에 띄움. 오버레이는 vMix/브라우저에서 LAN 주소로 접속.
//   - 패키징 시: 상태/업로드는 userData(WC_DATA_DIR), 위플랩 감시는 내장 크롬 사용.
// ============================================================
const { app, BrowserWindow, shell } = require("electron");
const { fork } = require("child_process");
const path = require("path");
const fs = require("fs");
const http = require("http");

const PORT = 8093;
const ROOT = path.join(__dirname, "..");
const SERVER = path.join(ROOT, "server.js");
let win = null;
let child = null;

// 패키징(exe) 시 resources/puppeteer-cache 에 내장된 크롬 실행파일 찾기
function bundledChromePath() {
  if (!app.isPackaged) return "";
  const base = path.join(process.resourcesPath, "puppeteer-cache", "chrome");
  try {
    for (const dir of fs.readdirSync(base)) {              // win64-<버전>
      const exe = path.join(base, dir, "chrome-win64", "chrome.exe");
      if (fs.existsSync(exe)) return exe;
    }
  } catch {}
  return "";
}

function startServer() {
  if (child) return;
  const chrome = bundledChromePath();
  child = fork(SERVER, [], {
    cwd: ROOT,
    silent: true,
    env: {
      ...process.env,
      ELECTRON_RUN_AS_NODE: "1",
      ...(app.isPackaged ? { WC_DATA_DIR: app.getPath("userData") } : {}),
      ...(chrome ? { PUPPETEER_EXECUTABLE_PATH: chrome } : {}),
    },
  });
  if (child.stdout) child.stdout.on("data", (d) => process.stdout.write(d));
  if (child.stderr) child.stderr.on("data", (d) => process.stderr.write(d));
  child.on("exit", (code) => { child = null; if (code) console.log("server exit", code); });
}

function waitForServer(cb, tries = 0) {
  const req = http.get(`http://localhost:${PORT}/state`, (res) => { res.destroy(); cb(); });
  req.on("error", () => { if (tries < 80) setTimeout(() => waitForServer(cb, tries + 1), 250); else cb(); });
}

function createWindow() {
  win = new BrowserWindow({
    width: 1320, height: 880,
    title: "wc-overlay 컨트롤",
    backgroundColor: "#14101a",
    webPreferences: { contextIsolation: true },
  });
  win.setMenuBarVisibility(false);
  waitForServer(() => win.loadURL(`http://localhost:${PORT}/control`));
  // 오버레이 등 새 창 링크는 기본 브라우저로
  win.webContents.setWindowOpenHandler(({ url }) => { shell.openExternal(url); return { action: "deny" }; });
  win.on("closed", () => { win = null; });
}

const lock = app.requestSingleInstanceLock();
if (!lock) { app.quit(); }
else {
  app.on("second-instance", () => { if (win) { if (win.isMinimized()) win.restore(); win.focus(); } });
  app.whenReady().then(() => { startServer(); createWindow(); });
  app.on("activate", () => { if (BrowserWindow.getAllWindows().length === 0) createWindow(); });
}
app.on("window-all-closed", () => { try { child && child.kill(); } catch {} app.quit(); });
app.on("before-quit", () => { try { child && child.kill(); } catch {} });
