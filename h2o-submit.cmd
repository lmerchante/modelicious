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
spark-submit --packages ai.h2o:sparkling-water-core_2.10:1.6.9 --driver-java-options "-Dhttp.proxyHost=proxy.cm.es -Dhttp.proxyPort=8080 -Dhttps.proxyHost=proxy.cm.es -Dhttps.proxyPort=8080" h2o\target\scala-2.10\H2OApp.jar %1 %1\%logfile% %1\environment.ini %1\variables.conf %2 %3
@echo off

endlocal