@ECHO ON
title CybosDataReader Start
set CONDAPATH=C:\Anaconda3
call %CONDAPATH%\Scripts\activate.bat %CONDAPATH%
call conda env list
d:
call cd D:\codex\py\metelsoft\CybosDataReader
rem call %CONDAPATH%\Scripts\activate.bat %ENVPATH%
call conda activate py38trade
python __init__.py

cmd.exe