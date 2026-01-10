:: 1. Turn off command echoing so only our explicit echo statements are shown
@echo off

:: 2. Set the console window title to the name of this .bat file (without extension)
title %~n0

:: 3. Get the directory where this .bat sits
set "SCRIPT_DIR=%~dp0"

:: 4. Add the 'pilot_project_1' folder to PYTHONPATH
:: %~dp0.. points to the parent of the script_runner folder
set "PYTHONPATH=%SCRIPT_DIR%..;%PYTHONPATH%"


:: 5. Going up 4 levels to find config: script_runner -> pilot_project_1 -> datastore -> etls -> datawarehouse
echo Loading environment...
call "%SCRIPT_DIR%..\..\..\..\config\local\setenv.bat"

echo The BASEDIR IS: BASEDIR=%BASEDIR%

echo The ETL IS: ETLS=%ETLS%

:: Print active environment (e.g. DEV / QA / PROD)
echo The ENVIRONMENT IS: ENVIRONMENT=%SCRIPTRUNNER_ENV%

echo Running ETL...
python "%SCRIPT_DIR%script_runner.py"
