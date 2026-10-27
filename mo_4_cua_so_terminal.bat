@echo off
chcp 65001 >nul
echo ===============================================================================
echo   KHOI DONG HE THONG PHAT HIEN GIAN LAN THEO 4 CUA SO TERMINAL DOC LAP
echo ===============================================================================

echo [0] Dam bao cum Docker (Kafka, Zookeeper, 1 NameNode, 3 DataNodes) dang chay...
cd /d "%~dp0"
docker compose up -d

set "PYTHON_EXE=%~dp0venv311\Scripts\python.exe"
set "TMP_DIR=%~dp0tmp"
set "UTF8_CMD=[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; [Console]::InputEncoding=[System.Text.Encoding]::UTF8; $OutputEncoding=[System.Text.Encoding]::UTF8; chcp 65001 | Out-Null; $env:PYTHONIOENCODING='utf-8';"

echo [1] Mo Cua so 1: WEB SERVER FLASK (http://127.0.0.1:5000)...
start "1. WEB FLASK SERVER (Port 5000)" powershell -NoExit -Command "%UTF8_CMD% $Host.UI.RawUI.WindowTitle='CUA SO 1 - WEB FLASK SERVER (http://127.0.0.1:5000)'; cd '%~dp0'; & '%PYTHON_EXE%' web/app.py"

timeout /t 2 >nul

echo [2] Mo Cua so 2: NHAN KAFKA -> GHI BRONZE & GOLD (Spark Streaming cham_diem.py)...
start "2. SPARK STREAMING: NHAN KAFKA -> GHI BRONZE & GOLD" powershell -NoExit -Command "%UTF8_CMD% $Host.UI.RawUI.WindowTitle='CUA SO 2 - SPARK STREAMING (cham_diem.py)'; cd '%~dp0'; $env:TEMP='%TMP_DIR%'; $env:TMP='%TMP_DIR%'; & '%PYTHON_EXE%' streaming/cham_diem.py --turbo"

timeout /t 3 >nul

echo [3] Mo Cua so 3: STATEFUL SILVER & BRONZE TO SILVER (cap_nhat_trang_thai.py)...
start "3. SPARK STATEFUL: CAP NHAT TRANG THAI SILVER" powershell -NoExit -Command "%UTF8_CMD% $Host.UI.RawUI.WindowTitle='CUA SO 3 - SPARK STATEFUL (cap_nhat_trang_thai.py)'; cd '%~dp0'; $env:TEMP='%TMP_DIR%'; $env:TMP='%TMP_DIR%'; & '%PYTHON_EXE%' streaming/cap_nhat_trang_thai.py"

timeout /t 5 >nul

echo [4] Mo Cua so 4: BAN GIAO DICH KAFKA PRODUCER (generator.py)...
start "4. KAFKA PRODUCER: BAN GIAO DICH TU CREDITCARD.CSV" powershell -NoExit -Command "%UTF8_CMD% $Host.UI.RawUI.WindowTitle='CUA SO 4 - KAFKA PRODUCER (generator.py)'; cd '%~dp0'; & '%PYTHON_EXE%' -u data/generator.py"

echo.
echo Da mo du 4 cua so Terminal!
pause
