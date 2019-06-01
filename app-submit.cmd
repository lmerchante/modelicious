@echo off
setlocal 
set config=%1
REM hours 0to9 do not add zeroes

for /f "delims= " %%x in ("%time:~0,2%") do set hour=%%x

for %%A in ("%config%") do (
    Set logfile=logfile-%date:~6,4%%date:~3,2%%date:~0,2%_%hour%%time:~3,2%%time:~6,2%.log
)

echo logfile is: %logfile%


@echo on
spark-submit app\target\scala-2.10\Application.jar %1 %1\%logfile% %1\environment.ini %1\variables.conf %2 %3
@echo off

endlocal