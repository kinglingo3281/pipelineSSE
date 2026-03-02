@echo off
echo Starting ngrok tunnel and SSE Server with Wrapper...

REM Start ngrok in separate window
cd /d "%~dp0"
start "Ngrok Tunnel" cmd /k ngrok.exe http 8080 --domain=your-domain.ngrok.app

REM Wait 3 seconds for ngrok to start
timeout /t 3 /nobreak

REM Start SSE Server via Wrapper (includes cleanup + crash recovery)
echo Starting SSE Server Wrapper with auto-restart and cleanup...
cd /d "%~dp0"
set JSON_CLEANUP_DAYS=1
echo Cleanup threshold set to %JSON_CLEANUP_DAYS% days
node sse-server-wrapper.js
pause
