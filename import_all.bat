@echo off
set "db=qw_16_unit_oper_data"
set "folder=C:\Users\TSiHao\Documents\Extracted NLA Analysis\Created_files"
set "mongoimport=C:\Program Files\MongoDB\Server\7.0\bin\mongoimport.exe"

:: Import lxx_data.csv into lxx_data collection
"%mongoimport%" --uri "mongodb://localhost:27017/%db%" --collection final_lxx_data --type csv --file "%folder%\final_lxx_data.csv" --headerline

:: Import psu_data.csv into psu_data collection
"%mongoimport%" --uri "mongodb://localhost:27017/%db%" --collection final_psu_data --type csv --file "%folder%\final_psu_data.csv" --headerline

echo Data import complete.
pause
