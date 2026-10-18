@echo off
chcp 65001 > nul
echo ===============================================================================
echo   DANG DAY TOAN BO DU AN LEN GITHUB (nguyenthanhphat-144/BigDaTa)
echo ===============================================================================

cd /d "%~dp0"
git push -u origin main --force

if %errorlevel% equ 0 (
    echo.
    echo [THANH CONG] Da day toan bo du an len: https://github.com/nguyenthanhphat-144/BigDaTa
) else (
    echo.
    echo [HUONG DAN] Neu trinh duyet mo ra yeu cau xac thuc, vui long dang nhap tai khoan nguyenthanhphat-144.
)

pause
