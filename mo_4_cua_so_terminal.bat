@echo off
color 0F
chcp 65001 >nul
echo ===============================================================================
echo   KHOI DONG HE THONG PHAT HIEN GIAN LAN THEO 4 CUA SO TERMINAL NEN DEN
echo ===============================================================================

echo [0] Dam bao cum Docker (Kafka, Zookeeper, 1 NameNode, 3 DataNodes) dang chay...
cd /d "%~dp0"
docker compose up -d

echo [0.1] Don dep tat ca tien trinh Python cu dang chay ngam de tranh xung dot lock file...
taskkill /F /IM python.exe >nul 2>&1
timeout /t 1 >nul

set "PYTHON_EXE=%~dp0venv311\Scripts\python.exe"
set "TMP_DIR=%~dp0tmp"

echo [1] Mo Cua so 1: WEB SERVER FLASK (http://127.0.0.1:5000)...
start "CUA SO 1 - WEB FLASK SERVER" cmd /k "title CUA SO 1 - WEB FLASK SERVER (http://127.0.0.1:5000) && color 0F && chcp 65001 >nul && cd /d "%~dp0" && "%PYTHON_EXE%" web/app.py"

timeout /t 2 >nul

echo [2] Mo Cua so 2: NHAN KAFKA -> GHI BRONZE & GOLD (Spark Streaming cham_diem.py)...
start "CUA SO 2 - SPARK STREAMING" cmd /k "title CUA SO 2 - SPARK STREAMING (cham_diem.py) && color 0F && chcp 65001 >nul && cd /d "%~dp0" && set "TEMP=%TMP_DIR%" && set "TMP=%TMP_DIR%" && "%PYTHON_EXE%" streaming/cham_diem.py --turbo"

timeout /t 3 >nul

echo [3] Mo Cua so 3: STATEFUL SILVER & BRONZE TO SILVER (cap_nhat_trang_thai.py)...
start "CUA SO 3 - SPARK STATEFUL" cmd /k "title CUA SO 3 - SPARK STATEFUL (cap_nhat_trang_thai.py) && color 0F && chcp 65001 >nul && cd /d "%~dp0" && set "TEMP=%TMP_DIR%" && set "TMP=%TMP_DIR%" && "%PYTHON_EXE%" streaming/cap_nhat_trang_thai.py --turbo"

timeout /t 4 >nul

echo [4] Mo Cua so 4: BAN GIAO DICH KAFKA PRODUCER (generator.py)...
start "CUA SO 4 - KAFKA PRODUCER" cmd /k "title CUA SO 4 - KAFKA PRODUCER (generator.py) && color 0F && chcp 65001 >nul && cd /d "%~dp0" && "%PYTHON_EXE%" -u data/generator.py"

echo.
echo Da mo du 4 cua so Terminal nen den (Command Prompt) chuan dep!
pause
