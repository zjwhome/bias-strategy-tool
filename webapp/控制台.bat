@echo off
chcp 65001 >nul
title 乖离率策略 - 控制台
cd /d "%~dp0"

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

rem ============================================================
rem  自动寻找可用的 Python（按优先级，谁先找到用谁）
rem    1) WorkBuddy 托管环境（开发者本机）
rem    2) 项目内虚拟环境 .venv / venv
rem    3) 系统 PATH 上的 python.exe
rem    4) Windows 官方启动器 py -3
rem  别人从 GitHub 下载后直接双击本文件即可，无需改任何路径。
rem ============================================================
set "PY="

if exist "%USERPROFILE%\.workbuddy\binaries\python\envs\default\Scripts\python.exe" (
  set "PY=%USERPROFILE%\.workbuddy\binaries\python\envs\default\Scripts\python.exe"
  goto found
)

if exist "%~dp0.venv\Scripts\python.exe" (
  set "PY=%~dp0.venv\Scripts\python.exe"
  goto found
)

if exist "%~dp0venv\Scripts\python.exe" (
  set "PY=%~dp0venv\Scripts\python.exe"
  goto found
)

rem ---- 系统 PATH 上的 python.exe ----
for %%C in (python.exe) do if not defined PY set "PY=%%~$PATH:C"
if defined PY goto found

rem ---- 官方启动器 py -3（解析成真实路径，避免命令里带空格） ----
where py >nul 2>nul
if not errorlevel 1 (
  for /f "delims=" %%P in ('py -3 -c "import sys; print(sys.executable)" 2^>nul') do set "PY=%%P"
)
if defined PY goto found

rem ============================================================
rem  没找到 Python
rem ============================================================
echo.
echo   [ERROR] 没有找到 Python，无法启动本工具。
echo.
echo   请先安装 Python 3.9 或更高版本（免费）：
echo       https://www.python.org/downloads/
echo.
echo   安装时务必勾选最下面那一项：
echo       [x] Add Python to PATH
echo.
echo   装好后重新双击本文件即可（首次会自动安装所需依赖）。
echo.
pause
exit /b 1

:found
	echo   使用 Python：%PY%
	echo.

rem ============================================================
rem  依赖自检：缺哪个就装哪个（首次运行会自动执行一次）
rem ============================================================
"%PY%" -c "import pandas,numpy,flask,akshare,requests" >nul 2>nul
if errorlevel 1 (
  echo ============================================================
  echo   首次运行：正在安装所需依赖
  echo   需要联网，大约 1~3 分钟，请耐心等待，不要关闭本窗口
  echo ============================================================
  echo.
  "%PY%" -m pip install --disable-pip-version-check -r "%~dp0..\requirements.txt"
  if errorlevel 1 (
    echo.
    echo   [ERROR] 依赖安装失败，请检查网络后重试。
    echo   也可以手动执行下面这条命令：
    echo.
    echo       "%PY%" -m pip install pandas numpy flask akshare requests
    echo.
    pause
    exit /b 1
  )
  echo.
  echo   依赖安装完成。
  echo.
)

"%PY%" control.py
if errorlevel 1 (
  echo.
  echo   The console exited with an error.
  pause
)
exit /b 0
