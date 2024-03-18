unit UPackDraft0FileExporter;

{$I PackMode.inc}

interface

uses
  UFile, UPackDraft0Shared;

type
  TPackDraft0FileExporter = type TPackDraft0FileTaskHandler;
  PPackDraft0FileExporter = ^TPackDraft0FileExporter;

procedure Start(var AExporter: TPackDraft0FileExporter; constref AFile: TFile; AMode: TPackDraft0FileHandleMode;
  const AIncludeIDs: TPackDraft0ItemIDArray; const AIncludePaths: TPackDraft0ItemPathArray;
  const ADestinationPath: TDirectoryPath; out AStatus: TPackDraft0Status); overload;

implementation

uses
  UNumber, UString, USystem, UList, UNumberHelp, UMemoryBlock, UFileHelp, UFileHandleHelp, UListHelp, UThread,
  USQLite3, USQLite3Help, UZstandard,
  UPackDraft0SQLite3VFS;

type
  TQueueItem = record
    ParentIndex: Ind; //Index in Queue.Items
    ID: TSQLite3ROWID;
    Kind: TPackDraft0ItemKind;
    Name: TPackDraft0ItemName;
    Created: Ind; //0: Not create, 1: Creating, 2: Created
  end;
  PQueueItem = ^TQueueItem;

  TQueueItemContent = record
    ContentPosition: Ind;
    ItemIndex: Ind; //Index in Queue.Items
    ItemPosition: Ind;
    Size: Siz;
  end;
  PQueueItemContent = ^TQueueItemContent;

  TQueueContent = record
    ID: TSQLite3ROWID;
    ItemContents: TList<TQueueItemContent>;
  end;
  PQueueContent = ^TQueueContent;

  TQueue = record
    Items: TList<TQueueItem>;
    Contents: TList<TQueueContent>;
    LastPickedContent: Ind;
  end;

  TProcessorsContext = object(TPackDraft0FileTaskHandlerProcessorsContext<TQueue>)
    DestinationPath: TDirectoryPath;
    class procedure Act(var AContext: TProcessorsContext; var AError: TPackDraft0Error;
      out AStatus: TPackDraft0Status); static;
  end;
  PProcessorsContext = ^TProcessorsContext;

class procedure TProcessorsContext.Act(var AContext: TProcessorsContext; var AError: TPackDraft0Error;
  out AStatus: TPackDraft0Status);

  function Initialize(out AConverter: PZSTD_DCtx; out AContent, AConvertedContent: TMemoryBlock;
    out ASelectContentByIDStatement: Psqlite3_stmt): Bool; overload;
  const
    SelectContentByIDSQLStatement: TSQLite3SQLStatement = 'SELECT Value FROM Content WHERE ID = ?;';
  begin
    AConverter := ZSTD_createDCtx;
    Result := AConverter <> nil;
    if not Result then
      Exit(HandleError(pd0sCanNotInitializeConverter, AStatus));
    try
      Result := Initialize(AContext.Connection, SelectContentByIDSQLStatement, ASelectContentByIDStatement,
        AError, AStatus);
      AContent := Default(TMemoryBlock); //Will be backed by SelectContentByIDSQLStatement
      AConvertedContent := Default(TMemoryBlock); //Will be allocated by Convert
    finally
      if not Result then
        ZSTD_freeDCtx(AConverter);
    end;
  end;

  procedure Finalize(var AConverter: PZSTD_DCtx; var AConvertedContent: TMemoryBlock;
  var ASelectContentByIDStatement: Psqlite3_stmt); overload;
  begin
    Clear(AConvertedContent);
    Finalize(ASelectContentByIDStatement);
    ZSTD_freeDCtx(AConverter);
  end;

  function Read(constref AQueueContent: TQueueContent; var AContent: TMemoryBlock;
    AStatement: Psqlite3_stmt): Bool; overload;
  var
    R: TSQLite3ResultCode;
  begin
    if AQueueContent.ID = 0 then //Folders and empty files
    begin
      AContent := Default(TMemoryBlock); //Clear without freeing memory
      Exit(True);
    end;

    Result := Reset(AStatement, R); //For any previous run
    Result := Result and BindAll(AStatement, AQueueContent.ID, R);
    Result := Result and Step(AStatement, R);
    Result := CheckSQLite3Result(AError, R, SQLITE_ROW, pd0sCanNotUseStatement, AStatus);
    if Result then //Keep Statement open to keep Content valid. It will be reset later by Load or finalized by Finalize
      ColumnBlob(AStatement, 0, AContent);
  end;

  function Convert(constref AQueueContent: TQueueContent; AConverter: PZSTD_DCtx; constref AContent: TMemoryBlock;
  var AConvertedContent: TMemoryBlock; out AResultContent: TMemoryBlock): Bool; overload;
  const
    MaxContentSize = 128 * 1024 * 1024;
  var
    S: unsignedlonglong;
    R: size_t;
  begin
    if ZSTD_isFrame(Data(AContent), Size(AContent)) = 0 then //Is compressed
    begin
      AResultContent := AContent;
      Exit(True);
    end;
    S := ZSTD_getFrameContentSize(Data(AContent), Size(AContent));
    if S > MaxContentSize then
      Exit(HandleError(AError, pd0sExceededContentSizeLimit, AQueueContent.ID, AStatus))
    else if (S = ZSTD_CONTENTSIZE_ERROR) or (S = ZSTD_CONTENTSIZE_UNKNOWN) then
      Exit(HandleError(AError, pd0sInvalidContentSize, AQueueContent.ID, AStatus));

    if S > Capacity(AConvertedContent) then //Allocate or expand memory
      if not Capacity(AConvertedContent, S, AStatus) then
        Exit(False);

    R := ZSTD_decompressDCtx(AConverter, Data(AConvertedContent), Capacity(AConvertedContent),
      Data(AContent), Size(AContent));
    if ZSTD_isError(R) = 1 then
      Exit(HandleError(AError, pd0sCanNotConvert, AQueueContent.ID, AStatus));
    Size(AConvertedContent, R);
    AResultContent := AConvertedContent;
    Result := True;
  end;

  function Write(constref AQueueContent: TQueueContent; constref AContent: TMemoryBlock;
    out ATotalSize: Siz): Bool; overload;

    //Either exists (Created = 2)
    // Or wait for it to exist (1) until it does
    // Or if it does not exist (0), then create
    //For files, return the handler
    function HandleExists(var AItem: TQueueItem; out APath: TFileSystemPath;
      out AFileHandler: TFileHandler): Bool;

      function Create: Bool; overload;
      var
        IT: PQueueItem;
        H: TFileHandler;
        P: TFileSystemPath;
      begin
        if AItem.ParentIndex <> -1 then //Parent
        begin
          IT := ItemPointer(AContext.Queue^.Items, AItem.ParentIndex);
          Result := HandleExists(IT^, P, H); //Not using H as Parent is a Folder
          if not Result then
            Exit;
        end
        else
          P := AContext.DestinationPath;
        APath := P + AItem.Name + Condition(AItem.Kind <> pd0ikFolder, '', PathDelimiter);
        case AItem.Kind of
          pd0ikFile:
          begin
            Result := Open(&File(APath), [ofoCreate, ofoWrite, ofoShareRead, ofoShareWrite],
              AFileHandler);
            if not Result then
              Exit(HandleOSError(AError, pd0sCanNotOpenFile, APath, AStatus));
          end;
          pd0ikFolder:
          begin
            Result := Create(Directory(APath));
            if not Result then
              Exit(HandleOSError(AError, pd0sCanNotCreateFolder, APath, AStatus));
          end
          else
            Result := False;
        end;
      end;

    var
      V: IPS;
    begin
      if AItem.Created = 0 then //Does not exists, ask for creating role
      begin
        V := InterlockedCompareExchange(AItem.Created, 1, 0); //Ask
        if V = 0 then //Then Created was 0 and now is 1, accepted
        begin
          Result := Create; //Create now, others will wait for this
          if Result then
            AItem.Created := 2; //Let others know it exists now
          Exit; //Either created or stop
        end; //One got it sooner, continue
      end; //Either 1, or 2

      while AItem.Created = 1 do //Other one is creating, wait for it or stop
      begin
        if CheckStopContext(AContext, AStatus) then
          Exit(False);
        Sleep(1); //Wait a bit
      end;

      //2: Exists
      APath := ItemFileSystemPath<TQueueItem>(AContext.DestinationPath, AItem, AContext.Queue^.Items, False);
      if AItem.Kind = pd0ikFolder then //Done if it is a folder
        Exit(True);

      //Open file
      Result := Open(&File(APath), [ofoOpen, ofoWrite, ofoShareRead, ofoShareWrite], AFileHandler);
      if not Result then
        Exit(HandleOSError(AError, pd0sCanNotOpenFile, APath, AStatus));
    end;

    function Write(constref AItemContent: TQueueItemContent): Bool; overload;
    var
      IT: PQueueItem;
      H: TFileHandler;
      P: TFileSystemPath;
    begin
      IT := ItemPointer(AContext.Queue^.Items, AItemContent.ItemIndex);
      Result := HandleExists(IT^, P, H);
      if Result and (IT^.Kind = pd0ikFile) then
      try
        if AItemContent.ItemPosition <> 0 then
          if not SeekTo(H, AItemContent.ItemPosition) then
            Exit(HandleOSError(AError, pd0sCanNotSeekFile, P, AStatus));

        if AItemContent.ContentPosition + AItemContent.Size > Size(AContent) then
          Exit(HandleError(AError, pd0sInvalidItemContent, AQueueContent.ID, AStatus));

        if AItemContent.Size <> 0 then //Empty file
          if not Write(H, Data(AContent) + AItemContent.ContentPosition, AItemContent.Size) then
            Exit(HandleOSError(AError, pd0sCanNotWriteFile, P, AStatus));
      finally
        Close(H);
      end;
    end;

  var
    I: Ind;
    IC: PQueueItemContent;
  begin
    Result := False;
    ATotalSize := 0;
    with AQueueContent do
      for I := First(ItemContents) to Last(ItemContents) do
      begin
        IC := ItemPointer(ItemContents, I);
        Result := Write(IC^);
        if (not Result) or CheckStopContext(AContext, AStatus) then
          Exit(False);
        ATotalSize += IC^.Size;
      end;
  end;

var
  CV: PZSTD_DCtx;
  C, CC: TMemoryBlock;
  SM: Psqlite3_stmt;
  QC: PQueueContent;
  RC: TMemoryBlock;
  TS: Siz;
begin
  AStatus := pd0sUnknown;
  if Initialize(CV, C, CC, SM) then
  try
    while (Next(AContext.Queue^, QC, AStatus)) and Read(QC^, C, SM) and Convert(QC^, CV, C, CC, RC) and
      Write(QC^, RC, TS) do
      AddDoneBytes(AContext.Statistics^, TS); //Statistics
  finally
    Finalize(CV, CC, SM);
  end;
end;

procedure ThreadGroupMethod(AIndex: Ind; AContext: Ptr); overload;
begin
  UPackDraft0Shared.ThreadGroupMethod<TProcessorsContext>(AIndex, PProcessorsContext(AContext)^);
end;

procedure Start(var AExporter: TPackDraft0FileExporter; constref AFile: TFile; AMode: TPackDraft0FileHandleMode;
  const AIncludeIDs: TPackDraft0ItemIDArray; const AIncludePaths: TPackDraft0ItemPathArray;
  const ADestinationPath: TDirectoryPath; out AStatus: TPackDraft0Status);

  function Prepare(AConnection: Psqlite3; out AQueue: TQueue): Bool; overload;

    function CreateAndSelectIndexedItemsTable(out AStatement: Psqlite3_stmt): Bool;
    var
      IIS: TSQLite3SQLStatement;
      R: TSQLite3ResultCode;
    begin
      IIS := IndexedItemsSQLite3SQLStatement(AIncludeIDs, AIncludePaths);
      Result := Execute(AConnection, ['CREATE TEMPORARY TABLE IndexedItems (I INTEGER PRIMARY KEY, PI, ID, Kind, Name);',
        'INSERT INTO IndexedItems SELECT I, PI, ID, Kind, Name FROM (' + IIS + ')'], True, R);
      if not Result then
        Exit(CheckSQLite3Result(AExporter.Error, R, pd0sCanNotProcessTransaction, AStatus));
      Result := Initialize(AConnection, 'SELECT PI, ID, Kind, Name FROM IndexedItems;', AStatement,
        AExporter.Error, AStatus);
    end;

    function SelectItems: Bool; //Create temporary table and select it. Keep the table for selecting ItemContents
    var
      STM: Psqlite3_stmt;
      R: TSQLite3ResultCode;
      IT: PQueueItem;
    begin
      Result := CreateAndSelectIndexedItemsTable(STM);
      if Result then
      try
        while Step(STM, SQLITE_ROW, R) do
        begin
          if not (AddEmpty<TQueueItem>(AQueue.Items, IT, AStatus)) then //Add Item
            Exit(False);
          Result := ColumnAll<Ind, TSQLite3ROWID, TPackDraft0ItemKind, TPackDraft0ItemName>(
            STM, IT^.ParentIndex, IT^.ID, IT^.Kind, IT^.Name, AStatus);
          Result := Result and IsValid(IT^.Name, AStatus);
          if (not Result) or CheckStop(AExporter, AStatus) then
            Exit(False);
        end;
        Result := CheckSQLite3Result(AExporter.Error, R, SQLITE_DONE, pd0sAbnormal, AStatus);
      finally
        Finalize(STM);
      end;
    end;

    //Check all children of root and delete them if allowed and needed to have a clean directory to export to
    function CheckExists: Bool;

      function Check(const APath: TFileSystemPath): Bool; overload;
      begin
        if Exists(FileSystemObject(APath)) then
          case AMode of
            pd0fhmCreate: Exit(HandleError(AExporter.Error, pd0sAlreadyExists, APath, AStatus));
            pd0fhmCreateOrOverwrite:
              if not Destroy(FileSystemObject(APath), @AExporter.Stopped) then
                if CheckStop(AExporter, AStatus) then
                  Exit(False)
                else
                  Exit(HandleOSError(AExporter.Error, pd0sCanNotOverwrite, APath, AStatus));
          end;
        Result := True;
      end;

    var
      I: Ind;
      IT: PQueueItem;
    begin
      Result := True; //Empty
      for I := First(AQueue.Items) to Last(AQueue.Items) do
      begin
        IT := ItemPointer(AQueue.Items, I);
        if IT^.ParentIndex = -1 then //Child of root or not
          if (not Check(ItemFileSystemPath<TQueueItem>(ADestinationPath, IT^, AQueue.Items, False))) or
            CheckStop(AExporter, AStatus) then
            Exit(False);
      end;
    end;

    function SelectItemContents: Bool;
    const
      SelectQueueSQLStatement: TSQLite3SQLStatement =
        'SELECT Content, ContentPosition, I AS ItemIndex, ItemPosition, Size FROM IndexedItems'
        + ' LEFT JOIN ItemContent ON IndexedItems.Kind = 0 AND IndexedItems.ID = ItemContent.Item'
        + ' ORDER BY Content, ContentPosition;';

      function AddToQueue(AStatement: Psqlite3_stmt; var ACurrentContent: PQueueContent): Bool;
      var
        CID: TSQLite3ROWID;
        IC: PQueueItemContent;
      begin
        Result := Column<TSQLite3ROWID>(AStatement, 0, CID, AStatus);
        if not Result then
          Exit;

        if (ACurrentContent = nil) or (CID <> ACurrentContent^.ID) then //Add a new content
          if not (AddEmpty<TQueueContent>(AQueue.Contents, ACurrentContent, AStatus)) then
            Exit
          else
            ACurrentContent^.ID := CID;

        //Add ItemContent
        if not (AddEmpty<TQueueItemContent>(ACurrentContent^.ItemContents, IC, AStatus)) then
          Exit;
        Result := ColumnAll<TSQLite3ROWID, Ind, Ind, Ind, Siz>(AStatement, CID,
          IC^.ContentPosition, IC^.ItemIndex, IC^.ItemPosition, IC^.Size, AStatus);
        if not Result then
          Exit;

        //Statistics
        AddTotalBytes(AExporter.Statistics, IC^.Size);
        AddItemCount(AExporter.Statistics, 1);

        Result := True;
      end;

    var
      STM: Psqlite3_stmt;
      R: TSQLite3ResultCode;
      CT: PQueueContent = nil;
    begin
      Result := Initialize(AConnection, SelectQueueSQLStatement, STM, AExporter.Error, AStatus);
      if Result then
      try
        while Step(STM, SQLITE_ROW, R) do
        begin
          Result := AddToQueue(STM, CT);
          if (not Result) or CheckStop(AExporter, AStatus) then
            Exit(False);
        end;
        Result := CheckSQLite3Result(AExporter.Error, R, SQLITE_DONE, pd0sAbnormal, AStatus);
      finally
        Finalize(STM);
      end;
    end;

  begin
    AQueue := Default(TQueue);
    Result := SelectItems and CheckExists and SelectItemContents;
    AQueue.LastPickedContent := -1;
  end;

  function Process(AConnection: Psqlite3; constref AQueue: TQueue): Bool; overload;
  var
    CX: TProcessorsContext;
  begin
    CX := Default(TProcessorsContext);
    CX.DestinationPath := ADestinationPath;
    Result := UPackDraft0Shared.Process<TQueue>(AExporter, CX, AConnection, ThreadGroupMethod, AQueue, AStatus);
  end;

var
  CN: Psqlite3;
  Q: TQueue;
begin
  AStatus := pd0sUnknown;
  if CheckExists(ADestinationPath, AStatus) and OpenConnection(AFile, True, CN, AExporter.Error, AStatus) then
  try
    if HandleOptions(CN, 0, AExporter.Error, AStatus) and Prepare(CN, Q) and Process(CN, Q) then
      AStatus := pd0sDone;
  finally
    CloseConnection(CN, AExporter.Error, AStatus);
  end;
end;

end.
