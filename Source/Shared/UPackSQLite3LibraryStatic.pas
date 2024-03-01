unit UPackSQLite3LibraryStatic;

{$I SCL.inc}

interface

implementation

uses
  {$IfDef Windows}
  UWindowsLibC,
  {$EndIF}
  USQLite3, UException;

  {$Link libsqlite3pack.o}

procedure CheckVersion;
const
  Expected = '3.45.1';
begin
  if sqlite3_libversion <> Expected then
    raise Exception.Create('Wrong SQLite version. ' + 'Expected: ' + Expected + ' but linked ' + sqlite3_libversion + '.');
end;

initialization
  CheckVersion;
end.
