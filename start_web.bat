@echo off
cd /d "%~dp0"
python web_server.py --local --port 8081 --open
pause
