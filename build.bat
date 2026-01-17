@echo off
echo Building NimbleDB...

REM Create output directories
if not exist "bin" mkdir bin
if not exist "bin\windows" mkdir bin\windows
if not exist "bin\linux" mkdir bin\linux
if not exist "bin\macos" mkdir bin\macos

REM Build for Windows (64-bit)
echo Building for Windows (amd64)...
go build -o bin\windows\nimbledb.exe
if %ERRORLEVEL% NEQ 0 (
    echo Failed to build for Windows
    exit /b 1
)

REM Build for Linux (64-bit)
echo Building for Linux (amd64)...
set GOOS=linux
set GOARCH=amd64
go build -o bin\linux\nimbledb
if %ERRORLEVEL% NEQ 0 (
    echo Failed to build for Linux
    exit /b 1
)

REM Build for macOS Intel (64-bit)
echo Building for macOS Intel (amd64)...
set GOOS=darwin
set GOARCH=amd64
go build -o bin\macos\nimbledb-intel
if %ERRORLEVEL% NEQ 0 (
    echo Failed to build for macOS Intel
    exit /b 1
)

REM Build for macOS ARM (M1/M2/M3)
echo Building for macOS ARM (arm64)...
set GOOS=darwin
set GOARCH=arm64
go build -o bin\macos\nimbledb-arm
if %ERRORLEVEL% NEQ 0 (
    echo Failed to build for macOS ARM
    exit /b 1
)

echo.
echo Build complete! Executables are in the bin folder:
echo - Windows: bin\windows\nimbledb.exe
echo - Linux:   bin\linux\nimbledb
echo - macOS:   bin\macos\nimbledb-intel and bin\macos\nimbledb-arm
