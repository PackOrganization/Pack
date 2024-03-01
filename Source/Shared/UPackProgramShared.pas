unit UPackProgramShared;

{$I PackMode.inc}

interface

uses
  UNumber, UString, UFile,
  UPackSQLite3LibraryStatic, UPackZstandardLibraryStatic;

type
  TPackVersion = (pvUnknown, pvDraft0);
  TPackProgramFileHandleMode = (ppfhmCreate, ppfhmCreateOrOverwrite);
  TPackProgramPress = (pppNone, pppHard);
  TPackProgramKind = (ppkDefault, ppkNo, ppkDebug);

const
  PackExtension = 'pack';
  PackVersion = pvDraft0;
  PackReadVersions = [pvDraft0];

function Version(constref AFile: TFile): TPackVersion; overload;

implementation

uses
  UFileHandleHelp, UMemory,
  UPackDraft0Shared;

function Version(constref AFile: TFile): TPackVersion;
var
  H: TFileHandler;
  P: Ptr;
  B: Bool;
begin
  try
    B := Open(AFile, [ofoRead, ofoShareRead, ofoShareWrite, ofoShareDelete], H);
    if B then
    try
      //Draft0
      P := ReadMemory(H, 16, B);
      if B then
      try
        B := CheckHeader(P, 16);
        if B then
          Result := pvDraft0;
      finally
        Deallocate(P);
      end;
    finally
      Close(H);
    end;
  finally
    if not B then
      Result := pvUnknown;
  end;
end;

end.
