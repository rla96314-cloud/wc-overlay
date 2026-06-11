@echo off
setlocal
cd /d "%~dp0"
chcp 65001 >nul
title wc-overlay - World Cup vMix overlay

echo ============================================
echo   wc-overlay - World Cup vMix overlay
echo ============================================
echo.

where node >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Node.js not found.
  echo Install Node.js LTS from https://nodejs.org then run again.
  pause
  exit /b 1
)

echo   Overlay (vMix Web Browser):  http://localhost:8093/
echo   Control page              :  http://localhost:8093/control
echo   Stop                      :  press Ctrl+C in this window
echo.

start "" http://localhost:8093/control
node server.js
pause
