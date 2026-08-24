@echo off
cd /d "C:\Users\natha\.claude\bid-crawler"
echo ===== %date% %time% ===== >> data\scheduled_run.log
"C:\Users\natha\AppData\Local\Python\pythoncore-3.14-64\Scripts\bid-crawler.exe" run --all --skip-fresh >> data\scheduled_run.log 2>&1
"C:\Users\natha\AppData\Local\Python\pythoncore-3.14-64\Scripts\bid-crawler.exe" sync >> data\scheduled_run.log 2>&1
