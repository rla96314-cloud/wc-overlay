#!/bin/bash
cd "$(dirname "$0")"
echo "⚽ wc-overlay 시작..."
if ! command -v node >/dev/null 2>&1; then
  echo "[오류] Node.js가 없습니다. https://nodejs.org 에서 설치 후 다시 실행하세요."
  read -n 1 -s -r -p "아무 키나 누르면 종료합니다..."
  exit 1
fi
if [ ! -d "node_modules/puppeteer" ]; then
  echo "최초 실행: 위플랩 감시용 패키지 설치 중 (puppeteer + Chromium, 수 분 소요)..."
  npm install
fi
node server.js
