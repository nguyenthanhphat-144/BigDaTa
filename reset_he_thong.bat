@echo off
chcp 65001 >nul
echo ===============================================================================
echo      LAM SACH VA KHOI PHUC HE THONG VE BAN DAU (RESET TO INITIAL STATE)
echo ===============================================================================
echo.
echo Tien trinh nay se:
echo   1. Xoa toan bo cac model phu (model_v2 .. model_v9), giu lai model_v1 goc.
echo   2. Xoa sach cac file checkpoint tam thoi cua Spark.
echo   3. Xoa du lieu giao dich cu trong Lakehouse (giu lai silver.tap_huan_luyen).
echo   4. Khoi tao lai cac bang Delta rong (schema chuan).
echo   5. Khoi dong lai Kafka va Zookeeper de don sach hang doi tin nhan.
echo.
set /p CONFIRM="Ban co chac chan muon reset toan bo he thong khong? (Y/N): "
if /i not "%CONFIRM%"=="Y" (
    echo [Huy] Khong thuc hien reset.
    pause
    exit /b
)

cd /d "%~dp0"
echo.
echo Dang thuc thi reset_he_thong.py...
venv311\Scripts\python.exe reset_he_thong.py

echo.
echo ===============================================================================
echo   RESET HOAN TAT! He thong da san sang chay moi tu dau.
echo ===============================================================================
pause
