unit UPackZstandardLibraryStatic;

{$I SCL.inc}

interface

implementation

uses
  UZstandard, UException;

  {$LinkLib libzstdpack.a}

procedure CheckVersion;
const
  Expected = '1.5.5';
begin
  if ZSTD_versionString <> Expected then
    raise Exception.Create('Wrong SQLite version. ' + 'Expected: ' + Expected + ' but linked ' + ZSTD_versionString + '.');
end;

initialization
  CheckVersion;
end.
