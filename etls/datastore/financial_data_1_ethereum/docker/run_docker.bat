:: Hide all commands from being printed to the console
@echo off

:: 1. Always switch to the directory where this .bat file is located
cd /d "%~dp0"

title %~n0
::See the title at the top

:: Print current working directory
echo Current directory: %cd%

:: Example variables you may want to use
set SCRIPT_NAME=docker_test
set DATA_DIR=/app/data
set CONFIG_DIR=/app/config

:: Print variable values
echo Script Name: %SCRIPT_NAME%
echo Data Dir: %DATA_DIR%
echo Config Dir: %CONFIG_DIR%

:: Print parameters
echo Parameter 1: %1
echo Parameter 2: %2

:: 2. Build the image
:: -t -> tag this image with a name
:: The dot . means to use the Dockerfile in the current directory.
echo.
echo Building Docker image...
docker build -t %SCRIPT_NAME% .

:: 3. Print a blank line
echo.

:: 4. Run the container
:: --rm -> delete the container automatically after it exits
echo Running container...
docker run --rm %SCRIPT_NAME% %1 %2

echo.
echo Done.
pause
