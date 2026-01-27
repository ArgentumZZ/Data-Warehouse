:: 1. Hide all commands from being printed to the console
@echo off

:: 2. Always switch to the directory where this .bat file is located
cd /d "%~dp0"

:: Enable 'setlocal' to ensure all variables defined in this script
:: are local to this execution and don't leak into the global environment.
setlocal

:: Define a start time
set "START_TIME=%date% %time%"

:: 4. Set the console window title (without the .bat extension)
title %~n0

:: 5. Capture command-line arguments passed to the script
:: %1 - The first argument passed to the script.
:: %~1 - The first argument, stripped of surrounding quotes.
:: set "VAR=..." - Safely assigns a value to a variable, protecting against special characters.

set "ARG1=%~1"
set "ARG2=%~2"
set "ARG3=%~3"
set "ARG4=%~4"
set "ARG5=%~5"

:: 5. Set variables for printing

:: Automatically extract the SCRIPT_NAME
:: set "SCRIPT_NAME=financial_data_1_ethereum"
for %%I in ("%cd%\..") do set "SCRIPT_NAME=%%~nI"

set "CURRENT_DIR=%cd%"

:: 6. Print a unified header
echo ============================================================
echo RUNNING SCRIPT:  %~n0
echo STARTED AT:      %START_TIME%
echo ============================================================
echo.
echo [PARAMETERS]
echo  Arguments passed: %*
echo.
echo [INFRASTRUCTURE]
echo  SCRIPT_NAME:       %SCRIPT_NAME%
echo  CURRENT_DIR:       %CURRENT_DIR%
echo ============================================================
echo.

:: 7. Build the image.
:: -t -> tag this image with a name (%SCRIPT_NAME%)
:: 7.1. The dot . means to use the Dockerfile in the current directory.
:: 7.2.  ..\..\..\.. -> Use the folder four levels above _docker.bat as the build context.
:: datawarehouse/
::    etls/
::        datastore/
::            project_name/
::                docker/
::                    _docker.bat
::                    Dockerfile
echo.
echo "Building Docker image..."
docker build -f Dockerfile -t %SCRIPT_NAME% ..\..\..\..
echo.

:: 8. Run the container
:: --rm -> delete the container automatically after it exits
:: --add-host=localhost:host-gateway:
::      - inside the container, treat localhost as the host machine).
::      - it allows the container to access services running on Windows.
:: local postgresql settings updated to liten to Docker
echo "Running the container..."
docker run --rm %SCRIPT_NAME% %*
::docker run --rm --add-host=localhost:host-gateway %SCRIPT_NAME% %*
:: docker run --rm --env-file "%ENV_PATH%" --add-host=localhost:host-gateway %SCRIPT_NAME% %*

:: 9. Capture exit code after Docker execution
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

:: 11. Return the exit code to the caller
exit /b %EXIT_CODE%