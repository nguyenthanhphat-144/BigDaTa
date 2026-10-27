# ===============================================================================
# KHOI DONG 4 CUA SO TERMINAL DOC LAP TU VSCODE HOAC POWERSHELL
# ===============================================================================
# Cau hinh UTF-8 toan dien chong loi font chu tieng Viet tren Windows Console
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::InputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
chcp 65001 | Out-Null
$env:PYTHONIOENCODING = "utf-8"

$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectDir

$utf8Preamble = "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; [Console]::InputEncoding = [System.Text.Encoding]::UTF8; `$OutputEncoding = [System.Text.Encoding]::UTF8; chcp 65001 | Out-Null; `$env:PYTHONIOENCODING = 'utf-8';"

Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host "   KHỞI ĐỘNG HỆ THỐNG 4 CỬA SỔ TERMINAL ĐỘC LẬP - FRAUD DETECTION" -ForegroundColor Yellow
Write-Host "===============================================================================" -ForegroundColor Cyan

# 0. Kiem tra va giai nen tap du lieu neu can
if (-not (Test-Path (Join-Path $projectDir "creditcard.csv"))) {
    $zipPath = Join-Path $projectDir "data\creditcard.csv.zip"
    if (Test-Path $zipPath) {
        Write-Host "`n[Tập dữ liệu] Đang giải nén creditcard.csv từ data\creditcard.csv.zip..." -ForegroundColor Yellow
        Expand-Archive -Path $zipPath -DestinationPath $projectDir -Force
        Write-Host "[Tập dữ liệu] Giải nén thành công!" -ForegroundColor Green
    }
}

# 1. Kiem tra thu muc tmp
$tmpDir = Join-Path $projectDir "tmp"
if (-not (Test-Path $tmpDir)) {
    New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null
}

# 2. Xac dinh trinh thong dich Python (uu tien venv311, venv, .venv hoac python)
$pythonExe = "python.exe"
if (Test-Path (Join-Path $projectDir "venv311\Scripts\python.exe")) {
    $pythonExe = Join-Path $projectDir "venv311\Scripts\python.exe"
} elseif (Test-Path (Join-Path $projectDir "venv\Scripts\python.exe")) {
    $pythonExe = Join-Path $projectDir "venv\Scripts\python.exe"
} elseif (Test-Path (Join-Path $projectDir ".venv\Scripts\python.exe")) {
    $pythonExe = Join-Path $projectDir ".venv\Scripts\python.exe"
}

# 3. Kiem tra cum Docker (Kafka, Zookeeper, HDFS)
Write-Host "`n[0/4] Kiểm tra cụm Docker (Kafka, Zookeeper, HDFS)..." -ForegroundColor Green
docker compose up -d

# 4. Cua so 1: Web Server Flask
Write-Host "[1/4] Đang mở Cửa sổ 1: WEB SERVER FLASK (http://127.0.0.1:5000)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$utf8Preamble `$Host.UI.RawUI.WindowTitle='CỬA SỔ 1 - WEB FLASK SERVER (http://127.0.0.1:5000)'; Set-Location '$projectDir'; & '$pythonExe' web/app.py"
Start-Sleep -Seconds 2

# 5. Cua so 2: Spark Streaming Cham Diem
Write-Host "[2/4] Đang mở Cửa sổ 2: SPARK STREAMING (cham_diem.py)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$utf8Preamble `$Host.UI.RawUI.WindowTitle='CỬA SỔ 2 - SPARK STREAMING (cham_diem.py)'; Set-Location '$projectDir'; `$env:TEMP='$tmpDir'; `$env:TMP='$tmpDir'; & '$pythonExe' streaming/cham_diem.py --turbo"
Start-Sleep -Seconds 3

# 6. Cua so 3: Spark Stateful Cap Nhat Trang Thai
Write-Host "[3/4] Đang mở Cửa sổ 3: SPARK STATEFUL (cap_nhat_trang_thai.py)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$utf8Preamble `$Host.UI.RawUI.WindowTitle='CỬA SỔ 3 - SPARK STATEFUL (cap_nhat_trang_thai.py)'; Set-Location '$projectDir'; `$env:TEMP='$tmpDir'; `$env:TMP='$tmpDir'; & '$pythonExe' streaming/cap_nhat_trang_thai.py --turbo"
Start-Sleep -Seconds 4

# 7. Cua so 4: Kafka Producer Phat Giao Dich
Write-Host "[4/4] Đang mở Cửa sổ 4: KAFKA PRODUCER (generator.py)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$utf8Preamble `$Host.UI.RawUI.WindowTitle='CỬA SỔ 4 - KAFKA PRODUCER (generator.py)'; Set-Location '$projectDir'; & '$pythonExe' -u data/generator.py"

Write-Host "`n[THÀNH CÔNG] Đã bật 4 cửa sổ terminal nổi trên màn hình với font tiếng Việt chuẩn UTF-8!" -ForegroundColor Yellow
Write-Host "Địa chỉ Web UI: http://127.0.0.1:5000" -ForegroundColor Cyan
