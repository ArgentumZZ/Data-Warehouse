@echo off
title ETL Runner

:: Get the directory where this .bat sits
set "SCRIPT_DIR=%~dp0"

:: Add the 'pilot_project_1' folder to PYTHONPATH
:: %~dp0.. points to the parent of the script_runner folder
set "PYTHONPATH=%SCRIPT_DIR%..;%PYTHONPATH%"

echo Loading environment...
:: Going up 4 levels to find config: script_runner -> pilot_project_1 -> datastore -> etls -> datawarehouse
call "%SCRIPT_DIR%..\..\..\..\config\local\setenv.bat"

echo BASEDIR=%BASEDIR%
echo ETLS=%ETLS%

echo Running ETL...
python "%SCRIPT_DIR%script_runner.py"

pause