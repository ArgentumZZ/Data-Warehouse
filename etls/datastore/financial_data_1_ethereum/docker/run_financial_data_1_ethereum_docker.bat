:: 1. Hide all commands from being printed to the console
@echo off

:: Always switch to the directory where this .bat file is located
cd /d "%~dp0"

:: Enable 'setlocal' to ensure all variables defined in this script
:: are local to this execution and don't leak into the global environment.
setlocal



:: Define start time to measure duration of the script
set "START_TIME=%date% %time%"

:: 2. Set the console window title (without .bat extension)
title %~n0

:: 3. Capture command-line arguments passed to the script
:: %1 - The first argument passed to the script.
:: %~1 - The first argument, stripped of surrounding quotes.
:: set "VAR=..." - Safely assigns a value to a variable, protecting against special characters.
Set "ARG1=%~1"
set "ARG2=%~2"

:: 4. This is a trick, if CMD splits 'config=FX' into %2=config and %3=FX,
:: we re-combine them into a single variable (=config=FX), where X is an integer.
if "%~2"=="config" if not "%~3"=="" (
    set "ARG2=%~2=%~3"
)

:: 5. Set  variables you may want to use
set "CURRENT_DIR=%cd%"
set "SCRIPT_NAME=financial_data_1_ethereum"
set "DATA_DIR=/app/data"
set "CONFIG_DIR=/app/config"

:: 6. Print a unified header
echo ============================================================
echo RUNNING SCRIPT:  %~n0
echo STARTED AT:      %START_TIME%
echo ============================================================
echo.
echo [PARAMETERS]
echo  Parameter 1 (date):             %ARG1%
echo  Parameter 2 (configuration):    %ARG2%
echo.
echo [INFRASTRUCTURE]
echo  SCRIPT_NAME:       %SCRIPT_NAME%
echo  CURRENT_DIR:       %CURRENT_DIR%
echo  DATA_DIR:          %DATA_DIR%
echo  CONFIG_DIR:        %CONFIG_DIR%
echo ============================================================
echo.

:: 7. Build the image.
:: -t -> tag this image with a name (%SCRIPT_NAME%)
:: The dot . means to use the Dockerfile in the current directory.
:: ..\..\..\.. -> Use the folder four levels above this script as the build context.
:: datawarehouse/
::    etls/
::        datastore/
::            project_name/
::                docker/
::                    _docker.bat   ← HERE
::                    Dockerfile
echo.
echo Building Docker image...
docker build -f Dockerfile -t %SCRIPT_NAME% ..\..\..\..
echo.

:: 8. Run the container
:: --rm -> delete the container automatically after it exits
:: --add-host=localhost:host-gateway:
:: Inside the container, treat localhost as the host machine.
:: It allows the container to access services running on Windows.
echo Running container...
docker run --rm --add-host=localhost:host-gateway %SCRIPT_NAME% "%~1" "%~2=%~3"
:: docker run --add-host=localhost:host-gateway %SCRIPT_NAME% "%~1" "%~2=%~3"

:: 9. Capture exit code IMMEDIATELY after Docker execution
set "EXIT_CODE=%ERRORLEVEL%"

:: 10. Final status report and exit code handling
echo ============================================================
if %EXIT_CODE% EQU 0 (
    echo SUCCESS: Docker container finished successfully.
    echo Return code: 0
) else (
    echo FAILURE: Docker container exited with error code %EXIT_CODE%
    echo Return code: 1
)
echo ============================================================

:: 11. Return the exit code to the caller
exit /b %EXIT_CODE%