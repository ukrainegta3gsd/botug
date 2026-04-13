@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo [1/4] Creating virtual environment...
    py -m venv .venv
)

call ".venv\Scripts\activate.bat"

echo [2/4] Installing dependencies...
python -m pip install --upgrade pip >nul
python -m pip install -r requirements.txt

echo [3/4] Starting bot...
python bot.py

echo [4/4] Bot stopped.
pause
