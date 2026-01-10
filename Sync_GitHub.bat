@echo off
echo Syncing with GitHub...
cd /d "C:\Users\ABCD\Documents\Antigravity Projects\Stock Market Ticker"
git add .
git commit -m "Auto-update: %date% %time%"
git push origin main
echo.
echo ===================================================
echo  Code Synced Successfully!
echo ===================================================
timeout /t 5
