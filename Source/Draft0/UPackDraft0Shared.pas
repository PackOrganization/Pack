unit UPackDraft0Shared;

{$I PackMode.inc}

interface

uses
  UNumber, UString, UFile, UException, UMemory, UMemoryHelp, USystem, USystemHelp, UMemoryBlock, UList,
  UThread, UThreadGroup, USQLite3, USQLite3Help;

type
  TPackDraft0HeaderString = array[0..3] of Char;

const
  PackDraft0HeaderString: TPackDraft0HeaderString = 'Pack';
  PackDraft0Extension = 'pack';
  PackDraft0Version: U16 = 1 shl 13 + 0; //Draft0: 8192

type
  TPackDraft0Status = (
    pd0sUnknown, pd0sAbnormal, pd0sDone, pd0sStopped, pd0sNoMemory,
    pd0sDoesNotExists, pd0sAlreadyExists, pd0sCanNotOverwrite, pd0sCanNotOpen, pd0sCanNotClose,
    pd0sCanNotProcessTransaction, pd0sCanNotInitializeStatement, pd0sCanNotUseStatement,
    pd0sCanNotSetOptions, pd0sCanNotReadFileAttributes, pd0sNotSupportedPath, pd0sCanNotOpenFolder,
    pd0sCanNotOpenFile, pd0sCanNotSeekFile, pd0sCanNotReadFile, pd0sCanNotWriteFile, pd0sInvalidValue,
    pd0sCanNotInitializeConverter, pd0sCanNotStartThread, pd0sCanNotConvert, pd0sExceededContentSizeLimit,
    pd0sInvalidContentSize, pd0sIrregularContentSize, pd0sInvalidItemContents, pd0sCanNotCreateFolder);

function Status(var ACurrentValue: TPackDraft0Status; ANewValue: TPackDraft0Status): Bool; inline; overload;

type
  TPackDraft0ItemPath = type Str;
  TPackDraft0ItemPathArray = array of TPackDraft0ItemPath;
  TPackDraft0FileHandleMode = (pd0fhmCreate, pd0fhmCreateOrOverwrite);
  TPackDraft0Press = (pd0pNone, pd0pHard);

  //Convert to 0 and 1 to store as special Serial Types of 8 and 9
  TPackDraft0ItemKind = (pd0ikNone = -1, pd0ikFile = 0, pd0ikFolder = 1);

function ToItemKind(AValue: TFileSystemObjectKind): TPackDraft0ItemKind; inline; overload;

type
  TPackDraft0ErrorReason = (pd0erUnknown, pd0erNone, pd0erInvalidFile, pd0erDoesNotExists, pd0erDiskIsFull);

  TPackDraft0Error = record
  private
    Reason: TPackDraft0ErrorReason;
    ItemPath: TFileSystemPath;
    RowID: TSQLite3ROWID;
    System: TSystemError;
    SQLite3: TSQLite3Error;
  end;
  PPackDraft0Error = ^TPackDraft0Error;

function Reason(constref AError: TPackDraft0Error): TPackDraft0ErrorReason; overload;
function UnknownReason(constref AError: TPackDraft0Error): Str; overload;
function Path(constref AError: TPackDraft0Error): TFileSystemPath; overload;
function RowID(constref AError: TPackDraft0Error): TSQLite3ROWID; overload;

function HandleError(AOnErrorStatus: TPackDraft0Status; var AStatus: TPackDraft0Status): Bool; overload;
function HandleError(var AError: TPackDraft0Error; AOnErrorStatus: TPackDraft0Status;
  const AErrorRowID: TSQLite3ROWID; var AStatus: TPackDraft0Status): Bool; overload;
function HandleError(var AError: TPackDraft0Error; AOnErrorStatus: TPackDraft0Status;
  const AErrorItemPath: TFileSystemPath; var AStatus: TPackDraft0Status): Bool; overload;
function HandleException(var AError: TPackDraft0Error; AException: Exception; var AStatus: TPackDraft0Status): Bool;
  overload;
function HandleOSError(var AError: TPackDraft0Error; AOnErrorStatus: TPackDraft0Status;
  const AErrorItemPath: TFileSystemPath; var AStatus: TPackDraft0Status): Bool; overload;
function CheckSQLite3Result(var AError: TPackDraft0Error;
  ASQLite3ResultCode, AExpectedSQLite3ResultCode: TSQLite3ResultCode;
  AOnErrorStatus: TPackDraft0Status; var AStatus: TPackDraft0Status): Bool; overload;
function CheckSQLite3Result(var AError: TPackDraft0Error; ASQLite3ResultCode: TSQLite3ResultCode;
  AOnErrorStatus: TPackDraft0Status; var AStatus: TPackDraft0Status): Bool; overload;
function AsText(constref AError: TPackDraft0Error; AStatus: TPackDraft0Status): Str; overload;

function Capacity(var AMemoryBlock: TMemoryBlock; ACapacity: Siz; var AStatus: TPackDraft0Status): Bool; inline; overload;

function Size<TQueueContent, TQueueItemContent>(constref AContent: TQueueContent): Siz; inline; overload;

function Initialize(AConnection: Psqlite3; const ASQLStatement: TSQLite3SQLStatement;
  out AStatement: Psqlite3_stmt; var AError: TPackDraft0Error; var AStatus: TPackDraft0Status): Bool; overload;
function ColumnValue<T>(AStatement: Psqlite3_stmt; AIndex: Ind; out AValue: T;
  var AStatus: TPackDraft0Status): Bool; overload;

function EnableSecurity(AConnection: Psqlite3; var AStatus: TPackDraft0Status): Bool; overload;
function Options(AConnection: Psqlite3; ASynchronousMode: TSQLite3SynchronousMode;
  ALockingMode: TSQLite3LockingMode; AJournalMode: TSQLite3JournalMode;
  ATempStore: TSQLite3TempStore; APageSize: TSQLite3PageSize;
  var AError: TPackDraft0Error; var AStatus: TPackDraft0Status): Bool; overload;

type
  TPackDraft0Statistics = record
  private
    TotalBytes, DoneBytes: UPS;
    ItemCount, DoneItemCount: UPS; //Todo: Improve or remove DoneItemCount
  end;
  PPackDraft0Statistics = ^TPackDraft0Statistics;

procedure AddTotalBytes(var AStatistics: TPackDraft0Statistics; AValue: UPS); inline; overload;
procedure AddDoneBytes(var AStatistics: TPackDraft0Statistics; AValue: UPS); inline; overload;
procedure AddItemCount(var AStatistics: TPackDraft0Statistics; AValue: UPS); inline; overload;
procedure AddDoneItemCount(var AStatistics: TPackDraft0Statistics; AValue: UPS); inline; overload;
function TotalBytes(constref AStatistics: TPackDraft0Statistics): UPS; inline; overload;
function DoneBytes(constref AStatistics: TPackDraft0Statistics): UPS; inline; overload;
function ItemCount(constref AStatistics: TPackDraft0Statistics): UPS; inline; overload;
function DoneItemCount(constref AStatistics: TPackDraft0Statistics): UPS; inline; overload;

type
  TPackDraft0FileTaskHandlerProcessor = object
    Error: TPackDraft0Error;
    Status: TPackDraft0Status;
  end;

  TPackDraft0FileTaskHandlerProcessorsContext<TQueue> = object
  protected
    StopRequest: PBool;
    Connection: Psqlite3;
    Queue: ^TQueue;
    Statistics: PPackDraft0Statistics;
    Stopped: UPS;
    Processors: array of TPackDraft0FileTaskHandlerProcessor;
    ThreadGroup: TThreadGroup;
  end;

function CheckStopContext<TContext>(constref AContext: TContext; var AStatus: TPackDraft0Status): Bool; inline; overload;
function Next<TQueue, PQueueContent>(var AQueue: TQueue; out AQueueContent: PQueueContent;
  var AStatus: TPackDraft0Status): Bool; overload;
procedure ThreadGroupMethod<TContext>(AIndex: Ind; var AContext: TContext); overload;

type
  TPackDraft0FileTaskHandler = object
    Stopped: Bool;
    Error: TPackDraft0Error;
    Statistics: TPackDraft0Statistics;
  end;
  PPackDraft0FileTaskHandler = ^TPackDraft0FileTaskHandler;

function CheckStop(constref AHandler: TPackDraft0FileTaskHandler; var AStatus: TPackDraft0Status): Bool;
  inline; overload;
function Process<TQueue>(var AHandler: TPackDraft0FileTaskHandler;
  var AContext: TPackDraft0FileTaskHandlerProcessorsContext<TQueue>; AConnection: Psqlite3;
  constref AMethod: TThreadGroupMethod; constref AQueue: TQueue; var AStatus: TPackDraft0Status): Bool; overload;
procedure Stop(var AHandler: TPackDraft0FileTaskHandler; out AStatus: TPackDraft0Status); overload;
function Error(constref AHandler: TPackDraft0FileTaskHandler): PPackDraft0Error; overload;
function Statistics(constref AHandler: TPackDraft0FileTaskHandler): PPackDraft0Statistics; overload;

function CheckHeader(AData: Ptr; ASize: Siz): Bool; overload;
function CheckHeader(const AFile: TFile): Bool; overload;
procedure WritePackHeader(AData: Ptr); overload;
procedure WriteSQLite3Header(AData: Ptr); overload;
function ToPackHeader(const AFile: TFile; out AStatus: TPackDraft0Status): Bool; overload;
function ToSQLite3Header(const AFile: TFile; out AStatus: TPackDraft0Status): Bool; overload;
procedure Transform(const ASource, ADestination: TFile; AToSQLite3OrPack: Bool;
  AMode: TPackDraft0FileHandleMode; out AStatus: TPackDraft0Status); overload;

implementation

uses
  UNumberHelp, UStringCheck, UStringHelp, UMemoryCompare, UFileHandleHelp, UFileHelp, UThreadHelp, USystemCPU;

//Only set the status if it is still unknown
// or if it is done and a sudden issue happens, like closing a transaction on a full disk
function Status(var ACurrentValue: TPackDraft0Status; ANewValue: TPackDraft0Status): Bool;
begin
  Result := ACurrentValue in [pd0sDone, pd0sUnknown];
  if Result then
    ACurrentValue := ANewValue;
end;

function ToItemKind(AValue: TFileSystemObjectKind): TPackDraft0ItemKind;
const
  Values: array[TFileSystemObjectKind] of TPackDraft0ItemKind = (pd0ikNone, pd0ikFile, pd0ikFolder);
begin
  Result := Values[AValue];
end;

function Reason(constref AError: TPackDraft0Error): TPackDraft0ErrorReason;
begin
  Result := AError.Reason;
end;

function UnknownReason(constref AError: TPackDraft0Error): Str;
begin
  with AError do
  begin
    if Reason <> pd0erUnknown then
      Exit('');
    Result := '';
    if HasError(System) then
      Result += 'System' + ToStr(Code(System));
    if HasError(SQLite3) then
      Result += 'Database' + ToStr(Code(SQLite3));
  end;
end;

function Path(constref AError: TPackDraft0Error): TFileSystemPath;
begin
  Result := AError.ItemPath;
end;

function RowID(constref AError: TPackDraft0Error): TSQLite3ROWID;
begin
  Result := AError.RowID;
end;

function HandleError(AOnErrorStatus: TPackDraft0Status; var AStatus: TPackDraft0Status): Bool;
begin
  Status(AStatus, AOnErrorStatus);
  Result := False;
end;

function HandleError(var AError: TPackDraft0Error; AOnErrorStatus: TPackDraft0Status; const AErrorRowID: TSQLite3ROWID;
  var AStatus: TPackDraft0Status): Bool;
begin
  if Status(AStatus, AOnErrorStatus) then
    AError.RowID := AErrorRowID;
  Result := False;
end;

function HandleError(var AError: TPackDraft0Error; AOnErrorStatus: TPackDraft0Status;
  const AErrorItemPath: TFileSystemPath; var AStatus: TPackDraft0Status): Bool;
begin
  if Status(AStatus, AOnErrorStatus) then
    AError.ItemPath := AErrorItemPath;
  Result := False;
end;

procedure HandleOSError(var AError: TPackDraft0Error); overload;
begin
  with AError do
  begin
    UpdateToLast(System);
    case Kind(System) of
      sekNone: Reason := pd0erNone;
      sekFileNotFound, sekPathNotFound: Reason := pd0erDoesNotExists;
      sekDiskIsFull: Reason := pd0erDiskIsFull;
      else
        Reason := pd0erUnknown;
    end;
  end;
end;

function HandleException(var AError: TPackDraft0Error; AException: Exception; var AStatus: TPackDraft0Status): Bool;
begin
  if AException is ESystemException then
    Code(AError.System, ESystemException(AException).Code)
  else
    Code(AError.System, -1);
  Message(AError.System, AException.Message);
  Result := HandleError(pd0sAbnormal, AStatus);
end;

function HandleOSError(var AError: TPackDraft0Error; AOnErrorStatus: TPackDraft0Status;
  const AErrorItemPath: TFileSystemPath; var AStatus: TPackDraft0Status): Bool;
begin
  if Status(AStatus, AOnErrorStatus) then
  begin
    HandleOSError(AError);
    AError.ItemPath := AErrorItemPath;
  end;
  Result := False;
end;

procedure HandleSQLite3Error(var AError: TPackDraft0Error; ASQLite3ResultCode: TSQLite3ResultCode);
begin
  Code(AError.SQLite3, ASQLite3ResultCode);
  Message(AError.SQLite3, ErrorMessage(ASQLite3ResultCode));
  case ASQLite3ResultCode of
    SQLITE_FULL: AError.Reason := pd0erDiskIsFull;
    SQLITE_NOTADB: AError.Reason := pd0erInvalidFile;
    else
      AError.Reason := pd0erUnknown;
  end;
end;

//Check and set AStatus and Handle SQLite3 Error, if there is an issue
function CheckSQLite3Result(var AError: TPackDraft0Error; ASQLite3ResultCode,
  AExpectedSQLite3ResultCode: TSQLite3ResultCode; AOnErrorStatus: TPackDraft0Status; var AStatus: TPackDraft0Status): Bool;
begin
  if ASQLite3ResultCode = AExpectedSQLite3ResultCode then
    Exit(True);
  if Status(AStatus, AOnErrorStatus) then
    HandleSQLite3Error(AError, ASQLite3ResultCode);
  Result := False;
end;

function CheckSQLite3Result(var AError: TPackDraft0Error; ASQLite3ResultCode: TSQLite3ResultCode;
  AOnErrorStatus: TPackDraft0Status; var AStatus: TPackDraft0Status): Bool;
begin
  Result := CheckSQLite3Result(AError, ASQLite3ResultCode, SQLITE_OK, AOnErrorStatus, AStatus);
end;

function AsText(constref AError: TPackDraft0Error; AStatus: TPackDraft0Status): Str;
var
  S: Str;
  R: TPackDraft0ErrorReason;
  ID: TSQLite3ROWID;
begin
  if AStatus = pd0sDone then
    Exit('');
  //Result := 'Error: ';
  Result := '';
  System.Str(AStatus, S);
  Result += S;
  R := Reason(AError);
  if R <> pd0erNone then
  begin
    if R <> pd0erUnknown then
    begin
      System.Str(R, S);
      Result += ', Reason: ' + S;
    end
    else
    begin
      S := UnknownReason(AError);
      if S <> '' then
        Result += ', Reason: ' + S;
    end;
    S := Path(AError);
    if S <> '' then
      Result += ', Path: ' + S;
    ID := RowID(AError);
    if ID <> 0 then
      Result += ', RowID: ' + ToStr(ID);
  end;
end;

function Capacity(var AMemoryBlock: TMemoryBlock; ACapacity: Siz; var AStatus: TPackDraft0Status): Bool;
begin
  Result := Capacity(AMemoryBlock, ACapacity);
  if not Result then
    Status(AStatus, pd0sNoMemory);
end;

//Size of Content based on the last ItemContent
function Size<TQueueContent, TQueueItemContent>(constref AContent: TQueueContent): Siz;
var
  IC: ^TQueueItemContent;
begin
  IC := ItemPointer(AContent.ItemContents, Last(AContent.ItemContents));
  Result := IC^.ContentPosition + IC^.Size;
end;

function Initialize(AConnection: Psqlite3; const ASQLStatement: TSQLite3SQLStatement; out AStatement: Psqlite3_stmt;
  var AError: TPackDraft0Error; var AStatus: TPackDraft0Status): Bool;
var
  R: TSQLite3ResultCode;
begin
  Result := Prepare(AConnection, ASQLStatement, AStatement, R);
  if not Result then
    Result := CheckSQLite3Result(AError, R, pd0sCanNotInitializeStatement, AStatus);
end;

function ColumnValue<T>(AStatement: Psqlite3_stmt; AIndex: Ind; out AValue: T; var AStatus: TPackDraft0Status): Bool;
begin
  Result := USQLite3Help.Column<T>(AStatement, AIndex, AValue);
  if not Result then
    AStatus := pd0sInvalidValue;
end;

function EnableSecurity(AConnection: Psqlite3; var AStatus: TPackDraft0Status): Bool;
begin
  Result := sqlite3_db_config(AConnection, SQLITE_DBCONFIG_TRUSTED_SCHEMA, 0, 0) = SQLITE_OK;
  Result := Result and (sqlite3_db_config(AConnection, SQLITE_DBCONFIG_ENABLE_TRIGGER, 0, 0) = SQLITE_OK);
  Result := Result and (sqlite3_db_config(AConnection, SQLITE_DBCONFIG_ENABLE_VIEW, 0, 0) = SQLITE_OK);
  if not Result then
    Status(AStatus, pd0sCanNotSetOptions);
end;

function Options(AConnection: Psqlite3; ASynchronousMode: TSQLite3SynchronousMode; ALockingMode: TSQLite3LockingMode;
  AJournalMode: TSQLite3JournalMode; ATempStore: TSQLite3TempStore; APageSize: TSQLite3PageSize;
  var AError: TPackDraft0Error; var AStatus: TPackDraft0Status): Bool;
var
  R: TSQLite3ResultCode;
begin
  Result := Options(AConnection, ASynchronousMode, ALockingMode, AJournalMode, ATempStore, R);
  if APageSize <> 0 then
    Result := Result and PageSize(AConnection, APageSize, [], R);
  Result := CheckSQLite3Result(AError, R, pd0sCanNotSetOptions, AStatus);
end;

procedure AddTotalBytes(var AStatistics: TPackDraft0Statistics; AValue: UPS);
begin
  InterlockedAdd(AStatistics.TotalBytes, AValue);
end;

procedure AddDoneBytes(var AStatistics: TPackDraft0Statistics; AValue: UPS);
begin
  InterlockedAdd(AStatistics.DoneBytes, AValue);
end;

procedure AddItemCount(var AStatistics: TPackDraft0Statistics; AValue: UPS);
begin
  InterlockedAdd(AStatistics.ItemCount, AValue);
end;

procedure AddDoneItemCount(var AStatistics: TPackDraft0Statistics; AValue: UPS);
begin
  InterlockedAdd(AStatistics.DoneItemCount, AValue);
end;

function TotalBytes(constref AStatistics: TPackDraft0Statistics): UPS;
begin
  Result := AStatistics.TotalBytes;
end;

function DoneBytes(constref AStatistics: TPackDraft0Statistics): UPS;
begin
  Result := AStatistics.DoneBytes;
end;

function ItemCount(constref AStatistics: TPackDraft0Statistics): UPS;
begin
  Result := AStatistics.ItemCount;
end;

function DoneItemCount(constref AStatistics: TPackDraft0Statistics): UPS;
begin
  Result := AStatistics.DoneItemCount;
end;

function CheckStopContext<TContext>(constref AContext: TContext; var AStatus: TPackDraft0Status): Bool;
begin
  Result := (AContext.Stopped > 0) or AContext.StopRequest^;
  if Result then
    Status(AStatus, pd0sStopped);
end;

function Next<TQueue, PQueueContent>(var AQueue: TQueue; out AQueueContent: PQueueContent; var AStatus: TPackDraft0Status
  ): Bool;
var
  I: Ind;
begin
  I := InterlockedIncrement(AQueue.LastPickedContent);
  Result := I <= Last(AQueue.Contents);
  if Result then
    AQueueContent := ItemPointer(AQueue.Contents, I)
  else
    Status(AStatus, pd0sDone);
end;

procedure ThreadGroupMethod<TContext>(AIndex: Ind; var AContext: TContext);
var
  E: TPackDraft0Error;
  S: TPackDraft0Status;
  SC: UPS;
begin
  S := pd0sUnknown;
  E := Default(TPackDraft0Error);
  try
    try
      AContext.Act(AContext, E, S);
    except
      on EX: Exception do
        HandleException(E, EX, S);
    end;
  finally
    if S <> pd0sDone then
    begin
      SC := InterlockedIncrement(AContext.Stopped); //Flag all others to stop
      if SC = 1 then //Keep only the first error
        AContext.Processors[AIndex].Error := E
      else
        S := pd0sStopped;
    end;
    AContext.Processors[AIndex].Status := S;
  end;
end;

function CheckStop(constref AHandler: TPackDraft0FileTaskHandler; var AStatus: TPackDraft0Status): Bool;
begin
  Result := AHandler.Stopped;
  if Result then
    Status(AStatus, pd0sStopped);
end;

//Compiler Issue: Process calling UThreadHelp.Create raises an exception, and this method helps
procedure CreateGroup(out AGroup: TThreadGroup; ACount: U8; constref AMethod: TThreadGroupMethod;
  AParameter: Ptr); overload;
begin
  UThreadHelp.Create(AGroup, ACount, AMethod, AParameter);
end;

function Process<TQueue>(var AHandler: TPackDraft0FileTaskHandler;
  var AContext: TPackDraft0FileTaskHandlerProcessorsContext<TQueue>;
  AConnection: Psqlite3; constref AMethod: TThreadGroupMethod; constref AQueue: TQueue;
  var AStatus: TPackDraft0Status): Bool;

  function Initialize: Bool; overload;

    function EffectiveParallelProcessorCount(AQueueCount: Siz): U8;
    var
      C: IPS;
    begin
      C := LogicalProcessorCount;
      C := C div 2; //50% of all
      C := Clamp(C, 1, Min(AQueueCount, High(U8))); //No more than Content count
      Result := C;
    end;

  var
    PPC: Siz;
  begin
    PPC := EffectiveParallelProcessorCount(Count(AQueue.Contents));
    AContext.StopRequest := @AHandler.Stopped;
    AContext.Connection := AConnection;
    AContext.Queue := @AQueue;
    AContext.Statistics := @AHandler.Statistics;
    SetLength(AContext.Processors, PPC);
    CreateGroup(AContext.ThreadGroup, PPC, AMethod, @AContext);
    Result := True;
  end;

  procedure Finalize; overload;
  begin
    Close(AContext.ThreadGroup);
    AContext.Processors := nil;
  end;

  function Act: Bool; overload;
  var
    I: Ind;
  begin
    Result := Execute(AContext.ThreadGroup);
    if not Result then
    begin
      Status(AStatus, pd0sCanNotStartThread);
      Exit;
    end;
    WaitFor(AContext.ThreadGroup);

    //Choose the first Processor that had issue
    for I := 0 to High(AContext.Processors) do
      if not (AContext.Processors[I].Status in [pd0sDone, pd0sStopped]) then
      begin
        Status(AStatus, AContext.Processors[I].Status);
        AHandler.Error := AContext.Processors[I].Error;
        Exit(False);
      end;

    //Check for stop request from outside. Return Stopped even if Process is fully done
    if AHandler.Stopped then
    begin
      Status(AStatus, pd0sStopped);
      Exit(False);
    end;
  end;

begin
  Result := Count(AQueue.Contents) = 0; //Empty
  if (not Result) and Initialize then
  try
    Result := Act;
  finally
    Finalize;
  end;
end;

procedure Stop(var AHandler: TPackDraft0FileTaskHandler; out AStatus: TPackDraft0Status);
begin
  if not AHandler.Stopped then
  begin
    AHandler.Stopped := True;
    AStatus := pd0sDone;
  end
  else
    AStatus := pd0sStopped;
end;

function Error(constref AHandler: TPackDraft0FileTaskHandler): PPackDraft0Error;
begin
  Result := @AHandler.Error;
end;

function Statistics(constref AHandler: TPackDraft0FileTaskHandler): PPackDraft0Statistics;
begin
  Result := @AHandler.Statistics;
end;

function CheckHeader(AData: Ptr; ASize: Siz): Bool;
var
  V: U16;
begin
  Result := False;
  if ASize < 16 then //At least 16 bytes for a valid header
    Exit;

  if not Check(PChar(AData), RStr(PackDraft0HeaderString)) then //Must start with Pack
    Exit;

  //Check Version
  V := Read<U16>(AData, 4);
  V := ToLittleEndian(V);
  if V <> PackDraft0Version then
    Exit;

  //Check 10 unused zero bytes
  Result := Compare(AData + 6, 10, 0);
end;

function CheckHeader(const AFile: TFile): Bool;
var
  H: TFileHandler;
  P: Ptr;
begin
  Result := Open(AFile, [ofoRead], H);
  if Result then
  try
    P := ReadMemory(H, 16, Result);
    if Result then
    try
      Result := CheckHeader(P, 16);
    finally
      Deallocate(P);
    end;
  finally
    Close(H);
  end;
end;

procedure WritePackHeader(AData: Ptr);
begin
  WriteString(AData, 0, PackDraft0HeaderString, False); //Header: 4 bytes
  Write<U16>(AData, 4, ToLittleEndian(PackDraft0Version)); //Version: 2 bytes as LittleEndian
  Fill(AData + 6, 10, 0); //Unused: 10 zero bytes
end;

procedure WriteSQLite3Header(AData: Ptr);
begin
  WriteString(AData, 0, SQLite3HeaderString, True);
end;

function ToPackHeader(const AFile: TFile; out AStatus: TPackDraft0Status): Bool;
var
  H: TFileHandler;
  P: Ptr;
begin
  Result := False;
  AStatus := pd0sUnknown;
  Result := Open(AFile, [ofoRead, ofoWrite], H);
  if not Result then
    Exit(Status(AStatus, pd0sCanNotOpenFile));
  try
    P := ReadMemory(H, 16, Result);
    if not Result then
    begin
      AStatus := pd0sCanNotReadFile;
      Exit;
    end;
    try
      WritePackHeader(P);
      SeekTo(H, 0);
      Result := Write(H, P, 16);
    finally
      Deallocate(P);
    end;
    if Result then
      AStatus := pd0sDone;
  finally
    Close(H);
  end;
end;

function ToSQLite3Header(const AFile: TFile; out AStatus: TPackDraft0Status): Bool;
var
  H: TFileHandler;
  P: Ptr;
begin
  AStatus := pd0sUnknown;
  Result := Open(AFile, [ofoRead, ofoWrite], H);
  if not Result then
    Exit(Status(AStatus, pd0sCanNotOpenFile));
  try
    P := ReadMemory(H, 16, Result);
    if not Result then
    begin
      AStatus := pd0sCanNotReadFile;
      Exit;
    end;
    try
      WriteSQLite3Header(P);
      SeekTo(H, 0);
      Result := Write(H, P, 16);
    finally
      Deallocate(P);
    end;
    if Result then
      AStatus := pd0sDone;
  finally
    Close(H);
  end;
end;

procedure Transform(const ASource, ADestination: TFile; AToSQLite3OrPack: Bool; AMode: TPackDraft0FileHandleMode;
  out AStatus: TPackDraft0Status);
var
  O: TMoveFileSystemObjectOptions;
  R: Bool;
begin
  AStatus := pd0sUnknown;

  if AToSQLite3OrPack then
    R := ToSQLite3Header(ASource, AStatus)
  else
    R := ToPackHeader(ASource, AStatus);
  if not R then
    Exit;

  if AMode = pd0fhmCreateOrOverwrite then
    O := [mfsooReplace]
  else
    O := [];

  if not UFile.Move(AsFileSystemObject(ASource), AsFileSystemObject(ADestination), O) then
    case LastSystemError of
      sekPathNotFound, sekFileNotFound: AStatus := pd0sDoesNotExists;
      sekAlreadyExists: AStatus := pd0sAlreadyExists;
      else
        AStatus := pd0sAbnormal;
    end;
end;

{$IfNDef Release}
initialization
  LogCallbacks;
{$EndIf}
end.
