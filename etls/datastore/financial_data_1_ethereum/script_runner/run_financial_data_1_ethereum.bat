:: 1. Turn off command echoing so only our explicit echo statements are shown
@echo off

:: 2. Set the console window title (without .bat extension)
title %~n0

:: 3 Generate a formatted timestamp
for /f "delims=" %%i in ('powershell -command "get-date -format 'ddd yyyy-MM-dd HH:mm:ss'"') do set "STAMP=%%i"

:: 4. Capture command-line arguments passed to the script
:: %1 - The first argument passed to the script.
:: %~1 - The first argument, stripped of surrounding quotes.
:: set "VAR=..." - Safely assigns a value to a variable, protecting against special characters.
set "ARG1=%~1"
set "ARG2=%~2"

:: This is a trick, if CMD split 'config=F20' into %2=config and %3=F20,
:: we re-combine them into a single variable.
if "%~2"=="config" if not "%~3"=="" (
    set "ARG2=%~2=%~3"
)

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

:: 8. PYTHONPATH enables imports, tells where else to look for shared code
:: Identify the project root (one level up from script_runner)
:: Set the Python search path strictly to this project
set "PROJECT_ROOT=%SCRIPT_DIR%.."
set "PYTHONPATH=%PROJECT_ROOT%"

:: 9. Print the Unified Header
echo ============================================================
echo RUNNING SCRIPT:  %~n0
echo STARTED AT:      %STAMP%
echo ============================================================
echo.
echo [PARAMETERS]
echo  Parameter 1 (date):             %ARG1%
echo  Parameter 2 (configuration):    %ARG2%
echo.
echo [INFRASTRUCTURE]
echo  BASE_DIR:       %BASEDIR%
echo  ETLS_DIR:       %ETLS%
echo  PYTHONPATH:     %PYTHONPATH%
echo  ACTIVE_ENV:     %SCRIPT_RUNNER_ENV%
echo ============================================================
echo.

echo Running the run_script.py ...
:: python "%SCRIPT_DIR%run_script.py" %*
:: where %* means “all arguments passed to the .bat file”
:: python "%SCRIPT_DIR%run_script.py" "%~1" "%~2"
python "%SCRIPT_DIR%run_script.py" "%ARG1%" "%ARG2%"


