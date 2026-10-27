@echo off
chcp 65001 >nul
echo ===============================================================================
echo   KHỞI ĐỘNG HỆ THỐNG PHÁT HIỆN GIAN LẬN THEO 4 CỬA SỔ TERMINAL ĐỘC LẬP
echo ===============================================================================

echo [0] Đảm bảo cụm Docker (Kafka, Zookeeper, 1 NameNode, 3 DataNodes) đang chạy...
cd /d "%~dp0"
docker compose up -d

set "PYTHON_EXE=%~dp0venv311\Scripts\python.exe"
set "TMP_DIR=%~dp0tmp"
set "UTF8_CMD=[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; [Console]::InputEncoding=[System.Text.Encoding]::UTF8; $OutputEncoding=[System.Text.Encoding]::UTF8; chcp 65001 | Out-Null; $env:PYTHONIOENCODING='utf-8';"

echo [1] Mở Cửa sổ 1: WEB SERVER FLASK (http://127.0.0.1:5000)...
start "1. WEB FLASK SERVER (Port 5000)" powershell -NoExit -Command "%UTF8_CMD% $Host.UI.RawUI.WindowTitle='CỬA SỔ 1 - WEB FLASK SERVER (http://127.0.0.1:5000)'; cd '%~dp0'; & '%PYTHON_EXE%' web/app.py"

timeout /t 2 >nul

echo [2] Mở Cửa sổ 2: NHẬN KAFKA -> GHI BRONZE & GOLD (Spark Streaming cham_diem.py)...
start "2. SPARK STREAMING: NHAN KAFKA -> GHI BRONZE & GOLD" powershell -NoExit -Command "%UTF8_CMD% $Host.UI.RawUI.WindowTitle='CỬA SỔ 2 - SPARK STREAMING (cham_diem.py)'; cd '%~dp0'; $env:TEMP='%TMP_DIR%'; $env:TMP='%TMP_DIR%'; & '%PYTHON_EXE%' streaming/cham_diem.py --turbo"

timeout /t 3 >nul

echo [3] Mở Cửa sổ 3: STATEFUL SILVER & BRONZE TO SILVER (cap_nhat_trang_thai.py)...
start "3. SPARK STATEFUL: CAP NHAT TRANG THAI SILVER" powershell -NoExit -Command "%UTF8_CMD% $Host.UI.RawUI.WindowTitle='CỬA SỔ 3 - SPARK STATEFUL (cap_nhat_trang_thai.py)'; cd '%~dp0'; $env:TEMP='%TMP_DIR%'; $env:TMP='%TMP_DIR%'; & '%PYTHON_EXE%' streaming/cap_nhat_trang_thai.py"

timeout /t 5 >nul

echo [4] Mở Cửa sổ 4: BẮN GIAO DỊCH KAFKA PRODUCER (generator.py)...
start "4. KAFKA PRODUCER: BAN GIAO DICH TU CREDITCARD.CSV" powershell -NoExit -Command "%UTF8_CMD% $Host.UI.RawUI.WindowTitle='CỬA SỔ 4 - KAFKA PRODUCER (generator.py)'; cd '%~dp0'; & '%PYTHON_EXE%' -u data/generator.py"

echo.
echo Đã mở đủ 4 cửa sổ Terminal với font tiếng Việt UTF-8 chuẩn xác!
pause
