{ This file was automatically created by Lazarus. Do not edit!
  This source is only used to compile and install the package.
 }

unit PackDraft0Package;

{$warn 5023 off : no warning about unused units}
interface

uses
  UPackDraft0Shared, UPackDraft0FileImporter, UPackDraft0FileExporter, UPackDraft0SQLite3VFS, LazarusPackageIntf;

implementation

procedure Register;
begin
end;

initialization
  RegisterPackage('PackDraft0Package', @Register);
end.
