@echo off
cd %~dp0\..
echo Activating virtual environment...
call .venv\Scripts\activate

echo Starting FastAPI server...
python src\api.py

pause
