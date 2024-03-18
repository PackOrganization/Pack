unit UPackProgramShared;

{$I PackMode.inc}

interface

uses
  UNumber, UString, UFile, UPackDraft0Shared,
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

type
  TPackDraft0CLILister = type TPackDraft0TaskHandler;

procedure Start(var ALister: TPackDraft0CLILister; constref AFile: TFile;
  const AIncludeIDs: TPackDraft0ItemIDArray; const AIncludePaths: TPackDraft0ItemPathArray;
  out AStatus: TPackDraft0Status); overload;

implementation

uses
  UStringHelp, UNumberHelp, UFileHandleHelp, UMemory, UList, UByteUnit, UByteUnitHelp,
  UPackDraft0Iterator;

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

procedure Start(var ALister: TPackDraft0CLILister; constref AFile: TFile; const AIncludeIDs: TPackDraft0ItemIDArray;
  const AIncludePaths: TPackDraft0ItemPathArray; out AStatus: TPackDraft0Status);

  function List(var AIterator: TPackDraft0Iterator): Bool;
  type
    TListPropertyKind = (lpkID, lpkParent, lpkKind, lpkName, lpkSize, lpkPath);
    TListProperties = array[TListPropertyKind] of TList<Str>;
    TListMaxLengths = array[TListPropertyKind] of Siz;
  const
    ListPropertyKindText: array[TListPropertyKind] of Str = ('ID', 'Parent', 'Kind', 'Name', 'Size', 'Path');
  var
    Ps: TListProperties;
    MLs: TListMaxLengths;
    I: Ind;
    K: TListPropertyKind;
  begin
    Result := False;
    Ps := Default(TListProperties);
    while Next(AIterator) do
    begin
      Add(Ps[lpkID], ToStr(ID(AIterator)));
      Add(Ps[lpkParent], ToStr(Parent(AIterator)));
      Add(Ps[lpkKind], PackDraft0ItemKindTest[Kind(AIterator)]);
      Add(Ps[lpkName], Name(AIterator));
      Add(Ps[lpkSize], ToFractionalByteString(Size(AIterator), busDecimal));
      Add(Ps[lpkPath], Path(AIterator));
      if CheckStop(ALister, AStatus) then
        Exit(False);
    end;
    if Count(Ps[lpkID]) = 0 then //Empty
      Exit(True);

    MLs := Default(TListMaxLengths);

    //Header sizes
    for K := Low(TListPropertyKind) to High(TListPropertyKind) do
      MLs[K] := Length(ListPropertyKindText[K]);

    //Values sizes
    for I := 0 to Last(Ps[lpkID]) do
    begin
      for K := Low(TListPropertyKind) to High(TListPropertyKind) do
        MLs[K] := Max(MLs[K], Length(Item(Ps[K], I)));
      if CheckStop(ALister, AStatus) then
        Exit(False);
    end;

    //Write Header
    for K := Low(TListPropertyKind) to High(TListPropertyKind) do
      System.Write(Padded(ListPropertyKindText[K], MLs[K], paRight), '  ');
    WriteLn;
    for K := Low(TListPropertyKind) to High(TListPropertyKind) do
      System.Write(StringOfChar('-', MLs[K]), '  ');
    WriteLn;

    //Write Values
    for I := 0 to Last(Ps[lpkID]) do
    begin
      for K := Low(TListPropertyKind) to High(TListPropertyKind) do
        System.Write(Padded(Item(Ps[K], I), MLs[K], paRight), '  ');
      WriteLn;
      if CheckStop(ALister, AStatus) then
        Exit(False);
    end;
    Result := True;
  end;

var
  ITR: TPackDraft0Iterator;
  ST: TPackDraft0Status;
begin
  AStatus := pd0sUnknown;
  Open(AFile, ITR, AIncludeIDs, AIncludePaths, ST);
  if ST = pd0sDone then
  try
    if List(ITR) then
      AStatus := pd0sDone;
  finally
    Close(ITR, ST);
  end;
  if ST <> pd0sDone then
  begin
    AStatus := ST;
    Copy(Error(ITR)^, Error(ALister)^);
  end;
end;

end.
