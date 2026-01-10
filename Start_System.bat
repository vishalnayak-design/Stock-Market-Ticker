@echo off
echo Starting Stock Analysis System...
cd /d "C:\Users\ABCD\Documents\Antigravity Projects\Stock Market Ticker"
docker-compose up -d
echo.
echo ===================================================
echo  SUCCESS! The system is running in the background.
echo  Dashboard: http://localhost:8501
echo ===================================================
echo.
pause
