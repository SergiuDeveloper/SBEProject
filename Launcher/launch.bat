@echo off
setlocal enabledelayedexpansion

if %1 == "" (
	goto invalid
)
if %2 == "" (
	goto invalid
)
if %3 == "" (
	goto invalid
)

set brokersCount=%1
set publishersCount=%2
set subscribersCount=%3

echo Launching %brokersCount% brokers, %publishersCount% publishers, and %subscribersCount% subscribers

start "Master" cmd /K "cd ../Master && python3 main.py"
timeout 1
for /l %%x in (1, 1, %brokersCount%) do (
	echo Launching Broker #%%x
	start "Broker" cmd /K "cd ../Broker/out/artifacts/Broker_jar && java -jar Broker.jar"
	timeout 2
)
	
for /l %%x in (1, 1, %subscribersCount%) do (
	echo Launching Subscriber #%%x
	start "Subscriber" cmd /K "cd ../Subscriber && python3 main.py"
	timeout 2
) 

for /l %%x in (1, 1, %publishersCount%) do (
	echo Launching Publisher #%%x
	start "Publisher" cmd /K "cd ../Publisher && python3 main.py"
	timeout 2
)

timeout 180
taskkill /IM cmd.exe /FI "WINDOWTITLE eq Master*"
taskkill /IM cmd.exe /FI "WINDOWTITLE eq Broker*"
taskkill /IM cmd.exe /FI "WINDOWTITLE eq Publisher*"
taskkill /IM cmd.exe /FI "WINDOWTITLE eq Subscriber*"
	
goto end
	
:invalid
	echo Invalid number of parameters. Expected format: {brokers} {publishers} {subscribers}
	
:end
