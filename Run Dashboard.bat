@echo off
TITLE Sales Dashboard Server
echo ==================================================
echo Starting the Sales Meeting Dashboard...
echo ==================================================
echo.
cd /d "%~dp0"

IF NOT EXIST venv\Scripts\activate.bat (
    echo [INFO] First time setup. Creating Python virtual environment...
    python -m venv venv
)

call venv\Scripts\activate.bat

echo [INFO] Checking and installing required packages... (This might take a minute or two)
pip install -r requirements.txt -q

echo.
echo [SUCCESS] Launching the Dashboard...
echo Please leave this black window open while you use the app!
echo.
python -m streamlit run app.py
echo.
echo Something went wrong or the app was closed.
pause
