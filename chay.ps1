# ===============================================================================
# KHOI DONG 4 CUA SO TERMINAL DOC LAP (NEN DEN - COMMAND PROMPT)
# ===============================================================================
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::InputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
chcp 65001 | Out-Null
$env:PYTHONIOENCODING = "utf-8"

$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectDir

Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host "   KHOI DONG HE THONG 4 CUA SO TERMINAL NEN DEN - FRAUD DETECTION" -ForegroundColor Yellow
Write-Host "===============================================================================" -ForegroundColor Cyan

# 0. Kiem tra va giai nen tap du lieu neu can
if (-not (Test-Path (Join-Path $projectDir "creditcard.csv"))) {
    $zipPath = Join-Path $projectDir "data\creditcard.csv.zip"
    if (Test-Path $zipPath) {
        Write-Host "`n[Tap du lieu] Dang giai nen creditcard.csv tu data\creditcard.csv.zip..." -ForegroundColor Yellow
        Expand-Archive -Path $zipPath -DestinationPath $projectDir -Force
        Write-Host "[Tap du lieu] Giai nen thanh cong!" -ForegroundColor Green
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

# 2.1. Don dep tat ca cac tien trinh Python cu dang chay ngam de tranh xung dot lock file
Write-Host "`n[Don dep] Dang kiem tra va dung cac tien trinh Python cu dang chay ngam..." -ForegroundColor Cyan
Stop-Process -Name python -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 800

# 3. Kiem tra cum Docker (Kafka, Zookeeper, HDFS)
Write-Host "`n[0/4] Kiem tra cum Docker (Kafka, Zookeeper, HDFS)..." -ForegroundColor Green
docker compose up -d

# 4. Cua so 1: Web Server Flask (Nen den - color 0F)
Write-Host "[1/4] Dang mo Cua so 1: WEB SERVER FLASK (http://127.0.0.1:5000)..." -ForegroundColor Green
Start-Process cmd -ArgumentList "/k", "title CUA SO 1 - WEB FLASK SERVER (http://127.0.0.1:5000) && color 0F && chcp 65001 >nul && cd /d `"$projectDir`" && `"$pythonExe`" web/app.py"
Start-Sleep -Seconds 2

# 5. Cua so 2: Spark Streaming Cham Diem (Nen den - color 0F)
Write-Host "[2/4] Dang mo Cua so 2: SPARK STREAMING (cham_diem.py)..." -ForegroundColor Green
Start-Process cmd -ArgumentList "/k", "title CUA SO 2 - SPARK STREAMING (cham_diem.py) && color 0F && chcp 65001 >nul && cd /d `"$projectDir`" && set TEMP=$tmpDir && set TMP=$tmpDir && `"$pythonExe`" streaming/cham_diem.py --turbo"
Start-Sleep -Seconds 3

# 6. Cua so 3: Spark Stateful Cap Nhat Trang Thai (Nen den - color 0F)
Write-Host "[3/4] Dang mo Cua so 3: SPARK STATEFUL (cap_nhat_trang_thai.py)..." -ForegroundColor Green
Start-Process cmd -ArgumentList "/k", "title CUA SO 3 - SPARK STATEFUL (cap_nhat_trang_thai.py) && color 0F && chcp 65001 >nul && cd /d `"$projectDir`" && set TEMP=$tmpDir && set TMP=$tmpDir && `"$pythonExe`" streaming/cap_nhat_trang_thai.py --turbo"
Start-Sleep -Seconds 4

# 7. Cua so 4: Kafka Producer Phat Giao Dich (Nen den - color 0F)
Write-Host "[4/4] Dang mo Cua so 4: KAFKA PRODUCER (generator.py)..." -ForegroundColor Green
Start-Process cmd -ArgumentList "/k", "title CUA SO 4 - KAFKA PRODUCER (generator.py) && color 0F && chcp 65001 >nul && cd /d `"$projectDir`" && `"$pythonExe`" -u data/generator.py"

Write-Host "`n[THANH CONG] Da bat 4 cua so terminal nen den (Command Prompt) chuan dep!" -ForegroundColor Yellow
Write-Host "Dia chi Web UI: http://127.0.0.1:5000" -ForegroundColor Cyan
