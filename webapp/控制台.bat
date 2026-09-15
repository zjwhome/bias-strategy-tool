@echo off
chcp 65001 >nul
title 乖离率策略 - 控制台
cd /d "%~dp0"

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
set "PY=%USERPROFILE%\.workbuddy\binaries\python\envs\default\Scripts\python.exe"

if not exist "%PY%" (
  echo.
  echo   [ERROR] Python runtime not found:
  echo     %PY%
  echo.
  echo   Please make sure the WorkBuddy Python environment exists.
  echo.
  pause
  exit /b 1
)

"%PY%" control.py
if errorlevel 1 (
  echo.
  echo   The console exited with an error.
  pause
)
