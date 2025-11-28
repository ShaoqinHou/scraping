@echo off
setlocal
cd /d "%~dp0"

echo [1/4] Create or activate local virtual environment...
if not exist .venv (
  python -m venv .venv
)
call .venv\Scripts\activate.bat

echo [2/4] Install project dependencies and PyInstaller...
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install pyinstaller

echo [3/4] Build EXE with PyInstaller...
pyinstaller --clean --noconfirm --name app ^
  --collect-all numpy ^
  --collect-all pandas ^
  --collect-all playwright ^
  --collect-all openai ^
  --add-data "templates;templates" ^
  --add-data "cn_regions_raw.json;." ^
  --add-data "inner_mongolia_projects.csv;." ^
  app.py

echo [4/4] Install Playwright browser into packaged app path...
set "PW_PKG_DIR=%CD%\dist\app\_internal\playwright\driver\package"
if exist "%PW_PKG_DIR%" (
  set "PLAYWRIGHT_BROWSERS_PATH=%PW_PKG_DIR%\.local-browsers"
  playwright install chromium
) else (
  echo WARNING: Playwright package directory not found: %PW_PKG_DIR%
)

if exist "%CD%\dist\app" (
  echo Create run_app.bat launcher...
  >"%CD%\dist\app\run_app.bat" (
    echo @echo off
    echo chcp 65001 ^>nul
    echo setlocal
    echo cd /d "%%~dp0"
    echo set "PLAYWRIGHT_BROWSERS_PATH=%%~dp0_internal\playwright\driver\package\.local-browsers"
    echo echo 应用已启动，请在浏览器打开: http://127.0.0.1:8000/ ^（Ctrl+左键点击或复制粘贴到浏览器地址栏^）
    echo echo 如需停止，请回到此窗口按 Ctrl+C，然后输入 Y 再回车。
    echo app.exe
  )
  echo Built app is in: %CD%\dist\app
) else (
  echo WARNING: dist\app not found; build may have failed.
)

echo.
echo Build complete. Distributable folder:
echo    %CD%\dist\app
echo.
echo To launch: double-click dist\\app\\run_app.bat
echo.
