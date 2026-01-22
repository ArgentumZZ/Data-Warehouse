:: 1. Hide all commands from being printed to the console
@echo off
:: Enable 'setlocal' to ensure all variables defined in this script
:: are local to this execution and don't leak into the global environment.
setlocal

:: 2. Set the console window title (without .bat extension)
title %~n0

:: 3 Generate a formatted timestamp (e.g. Sun 01/18/2026 11:29:48.74)
set "STAMP=%date% %time%"

:: 4. Capture command-line arguments passed to the script
:: %1 - The first argument passed to the script.
:: %~1 - The first argument, stripped of surrounding quotes.
:: set "VAR=..." - Safely assigns a value to a variable, protecting against special characters.

set "ARG1=%~1"
set "ARG2=%~2"
set "ARG3=%~3"
set "ARG4=%~4"
set "ARG5=%~5"

:: 5. Get the directory where this .bat sits.
:: %~dp0 - absolute path of the folder where the .bat file is sitting
:: For example, C:\...\datawarehouse\etls\datastore\project_1\script_runner\

:: %~dp0. - The current folder (same as %~dp0)
:: For example, C:\...\datawarehouse\etls\datastore\project_1\script_runner\.

:: %~dp0.. - Points to the parent folder (of the script_runner folder)
:: For example, C:\...\datawarehouse\etls\datastore\project_folder\

:: %~dp0..\.. - Points to the grandparent folder (...\datastore\)
:: %~dp0..\..\.. - Points to the great-grandparent folder (...\etls\)
set "SCRIPT_DIR=%~dp0"

:: 6. Store the path to the config file in a variable and load the external environment config.
:: Going up 4 levels to find config: script_runner -> project -> datastore -> etls -> datawarehouse
set "CONFIG_PATH=%SCRIPT_DIR%..\..\..\..\config\local\setenv.bat"

:: Call the variable
echo Loading environment...
call "%CONFIG_PATH%"

:: 7. Set environment (e.g. DEV / QA / PROD), base and etls directories
set "ENVIRONMENT=%SCRIPT_RUNNER_ENV%"
set "BASE_DIR=%BASEDIR%"
set "ETLS_DIR=%ETLS%"

:: Get the path of the active python executable,
:: venv is in the datawarehouse folder (=%BASE_DIR%)
set "VENV_PATH=%BASE_DIR%\venv\Scripts\activate.bat"

:: Check if the path exists and call it
if exist "%VENV_PATH%" (
    call "%VENV_PATH%"
    :: Force the script to use the venv's python executable
    set "PYTHON_EXE=%BASE_DIR%\venv\Scripts\python.exe"
) else (
    echo [ERROR] Virtual environment not found at: %VENV_PATH%
    exit /b 1
)

:: Get the python version string from the executable.
for /f "tokens=*" %%v in ('"%PYTHON_EXE%" --version 2^>^&1') do set "PYTHON_VERSION=%%v"

:: 8. PYTHONPATH enables imports, tells where else to look for shared code.
:: Level 1 (..): Point to project_folder
:: Level 3 (..\..\..): Point to etls for 'utilities' and 'connectors'
set "PYTHONPATH=%SCRIPT_DIR%..;%SCRIPT_DIR%..\..\.."

:: 9. Print a unified header
echo ============================================================
echo RUNNING SCRIPT:  %~n0
echo STARTED AT:      %STAMP%
echo ============================================================
echo.
echo [PARAMETERS]
echo  Arguments passed: %*
echo.
echo [INFRASTRUCTURE]
echo  BASE_DIR:       %BASEDIR%
echo  ETLS_DIR:       %ETLS%
echo  PYTHONPATH:     %PYTHONPATH%
echo  ACTIVE_ENV:     %ENVIRONMENT%
echo  PYTHON_EXE:     %PYTHON_EXE%
echo  PYTHON_VERSION: %PYTHON_VERSION%
echo ============================================================
echo.

:: 10. Check if run_script.py exists.
if not exist "%SCRIPT_DIR%run_script.py" (
    echo [ERROR] Script not found: %SCRIPT_DIR%run_script.py
    exit /b 1
)

:: 11. Run the program
echo Running the run_script.py ...
:: python "%SCRIPT_DIR%run_script.py" %* , where %* means “all arguments passed to the .bat file”
:: python "%SCRIPT_DIR%run_script.py" "%~1" "%~2"
:: "%PYTHON_EXE%" "%SCRIPT_DIR%run_script.py" "%ARG1%" "%ARG2%"
"%PYTHON_EXE%" "%SCRIPT_DIR%run_script.py" %*
echo.

:: 12. Capture exit code IMMEDIATELY after Python execution
:: We do this before any other commands to ensure %ERRORLEVEL% isn't overwritten.
set "EXIT_CODE=%ERRORLEVEL%"

:: 13. Final status report and exit code handling

echo ============================================================
if %EXIT_CODE% EQU 0 (
    echo SUCCESS: Script finished successfully.
    echo Return code: 0
) else (
    echo FAILURE: Script exited with error code %EXIT_CODE%
    echo Return code: 1
)

:: 15. Return the exit code to the caller (e.g., Task Scheduler or another .bat)
exit /b %EXIT_CODE%