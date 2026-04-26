
# Auto-Sync Script for Stock SIP System
# Run this on your Windows machine (not inside Docker)

$repoPath = Get-Location
$dataPath = Join-Path $repoPath "stock_ticker\data"

Write-Host "Monitoring $dataPath for changes..." -ForegroundColor Cyan

while ($true) {
    $status = git status --porcelain
    if ($status) {
        Write-Host "Changes detected. Syncing..." -ForegroundColor Yellow
        
        # Add config to handle eventual CRLF warnings
        git config core.autocrlf true
        
        git pull --rebase
        git add .
        git commit -m "Auto-sync: Data update $(Get-Date -Format 'yyyy-MM-dd HH:mm')"
        git push
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Sync Complete! ✅" -ForegroundColor Green
        } else {
            Write-Host "Sync Failed. Check internet or git credentials." -ForegroundColor Red
        }
    } else {
        Write-Host "No changes. Sleeping..." -ForegroundColor Gray
    }
    
    Start-Sleep -Seconds 60
}
