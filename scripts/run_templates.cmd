@echo off 
setlocal ENABLEEXTENSIONS EnableDelayedExpansion

set thedir=%1

pushd %~dp0

@echo off
for /r %thedir% %%f in (*.erb) do (
	echo %%f
	call erb %%f > %thedir%%%~nf || goto :error
)
goto :eof

:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%

:eof

popd
endlocal
