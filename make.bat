@echo off

SET ERLC="C:\Workspace\MyPrograms\erl10.0.1\bin\erlc"

FOR %%f in (src\*.erl) DO %ERLC% -W -pa .\ebin\ -I.\include\ -o .\ebin\ "%%f"

COPY .\src\meteor.app.src .\ebin\meteor.app