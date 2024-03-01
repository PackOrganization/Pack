unit UPackDraft0FileExporter;

{$I PackMode.inc}

interface

uses
  UFile, UPackDraft0Shared;

type
  TPackDraft0FileExporter = type TPackDraft0FileTaskHandler;
  PPackDraft0FileExporter = ^TPackDraft0FileExporter;

procedure Start(var AExporter: TPackDraft0FileExporter; constref AFile: TFile; AMode: TPackDraft0FileHandleMode;
  const ADestinationPath: TDirectoryPath; out AStatus: TPackDraft0Status); overload;

implementation

uses
  UNumber, UString, USystem, UList, UNumberHelp, UMemoryBlock, UFileHelp, UFileHandleHelp, UFilePathHelp, UListHelp,
  USQLite3, USQLite3Help, UZstandard,
  UPackDraft0SQLite3VFS;

type
  TQueueItemContent = record
    Kind: TPackDraft0ItemKind;
    ContentPosition: Ind;
    Path: TPackDraft0ItemPath;
    FilePosition: Ind;
    Size: Siz;
  end;
  PQueueItemContent = ^TQueueItemContent;

  TQueueContent = record
    ID: TSQLite3ROWID;
    ItemContents: TList<TQueueItemContent>;
    TotalSize: Siz;
  end;
  PQueueContent = ^TQueueContent;

  TQueue = record
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
    Result := Result and Bind(AStatement, AQueueContent.ID, R);
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

  function Write(constref AQueueContent: TQueueContent; constref AContent: TMemoryBlock): Bool; overload;

    function CreateDirectory(constref AItemContent: TQueueItemContent): Bool; overload;
    var
      P: TDirectoryPath;
    begin
      P := AContext.DestinationPath + AItemContent.Path;
      Result := CreateDirectoryAndParents(Directory(P));
      if not Result then
        Exit(HandleOSError(AError, pd0sCanNotCreateFolder, P, AStatus));
    end;

    function WriteItemContent(constref AItemContent: TQueueItemContent): Bool; overload;
    var
      P: TFilePath;
      H: TFileHandler;
    begin
      P := AContext.DestinationPath + AItemContent.Path;
      Result := Open(&File(P), [ofoCreate, ofoWrite, ofoShareRead, ofoShareWrite], H);
      if (not Result) and (LastSystemError = sekPathNotFound) then //Maybe problem is the path
      begin
        Result := CreateDirectoryAndParents(Directory(Parent(P))); //Try to ensure it exists
        if not Result then
          Exit(HandleOSError(AError, pd0sCanNotCreateFolder, P, AStatus));

        Result := Open(&File(P), [ofoCreate, ofoWrite, ofoShareRead, ofoShareWrite], H); //Try again
        if not Result then
          Exit(HandleOSError(AError, pd0sCanNotOpenFile, P, AStatus));
      end;
      try
        if AItemContent.FilePosition <> 0 then
          if not SeekTo(H, AItemContent.FilePosition) then
            Exit(HandleOSError(AError, pd0sCanNotSeekFile, P, AStatus));
        if AItemContent.Size <> 0 then //Empty file
          if not Write(H, Data(AContent) + AItemContent.ContentPosition, AItemContent.Size) then
            Exit(HandleOSError(AError, pd0sCanNotWriteFile, P, AStatus));
      finally
        Close(H);
      end;
    end;

  var
    IC: PQueueItemContent;
    I: Ind;
  begin
    Result := False;
    with AQueueContent do
    begin
      if TotalSize <> Size(AContent) then
        Exit(HandleError(AError, pd0sIrregularContentSize, AQueueContent.ID, AStatus));
      for I := First(ItemContents) to Last(ItemContents) do
      begin
        if CheckStopContext(AContext, AStatus) then
          Exit(False);
        IC := ItemPointer(ItemContents, I);
        case IC^.Kind of
          pd0ikFile: Result := WriteItemContent(IC^);
          pd0ikFolder: Result := CreateDirectory(IC^);
          else;
        end;
        if not Result then
          Exit;
      end;
    end;
  end;

var
  CV: PZSTD_DCtx;
  C, CC: TMemoryBlock;
  SM: Psqlite3_stmt;
  QC: PQueueContent;
  RC: TMemoryBlock;
begin
  AStatus := pd0sUnknown;
  if Initialize(CV, C, CC, SM) then
  try
    while (Next(AContext.Queue^, QC, AStatus)) and Read(QC^, C, SM) and Convert(QC^, CV, C, CC, RC) and Write(QC^, RC) do
      AddDoneBytes(AContext.Statistics^, QC^.TotalSize); //Statistics
  finally
    Finalize(CV, CC, SM);
  end;
end;

procedure ThreadGroupMethod(AIndex: Ind; AContext: Ptr); overload;
begin
  UPackDraft0Shared.ThreadGroupMethod<TProcessorsContext>(AIndex, PProcessorsContext(AContext)^);
end;

procedure Start(var AExporter: TPackDraft0FileExporter; constref AFile: TFile; AMode: TPackDraft0FileHandleMode;
  const ADestinationPath: TDirectoryPath; out AStatus: TPackDraft0Status);

  function HandleExists: Bool;
  begin
    Result := Exists(FileSystemObject(ADestinationPath));
    if not Result then
      HandleError(pd0sDoesNotExists, AStatus);
  end;

  function OpenConnection(out AConnection: Psqlite3): Bool;
  var
    R: TSQLite3ResultCode;
  begin
    R := Open(Path(AFile), SQLITE_OPEN_READONLY or SQLITE_OPEN_EXRESCODE or SQLITE_OPEN_FULLMUTEX, AConnection);
    Result := CheckSQLite3Result(AExporter.Error, R, pd0sCanNotOpen, AStatus);
  end;

  function CloseConnection(AConnection: Psqlite3): Bool;
  var
    R: TSQLite3ResultCode;
  begin
    R := Close(AConnection);
    Result := CheckSQLite3Result(AExporter.Error, R, pd0sCanNotClose, AStatus);
  end;

  function HandleOptions(AConnection: Psqlite3): Bool;
  begin
    Result := EnableSecurity(AConnection, AStatus);
    Result := Result and Options(AConnection, s3smOff, s3lmExclusive, s3jmOff, s3tmMemory, 0, AExporter.Error, AStatus);
  end;

  function Prepare(AConnection: Psqlite3; out AQueue: TQueue): Bool; overload;

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

    const
      SelectItemsSQLStatement: TSQLite3SQLStatement =
        'SELECT Name || IIF(Item.Kind = 1, ''/'', '''') AS Path FROM Item'
        + ' WHERE Parent = 0 AND Kind IN (0, 1) AND Length(Name) <> 0 AND InStr(Name, ''/'') = 0;';
    var
      STM: Psqlite3_stmt;
      R: TSQLite3ResultCode;
      P: TFileSystemPath;
    begin
      Result := Initialize(AConnection, SelectItemsSQLStatement, STM, AExporter.Error, AStatus);
      if Result then
      try
        while Step(STM, SQLITE_ROW, R) do
        begin
          Result := ColumnValue<TFileSystemPath>(STM, 0, P, AStatus);
          Result := Result and Check(ADestinationPath + P);
          if (not Result) or CheckStop(AExporter, AStatus) then
            Exit(False);
        end;
        Result := CheckSQLite3Result(AExporter.Error, R, SQLITE_DONE, pd0sAbnormal, AStatus);
      finally
        Finalize(STM);
      end;
    end;

    function SelectQueue: Bool;
    const
      SelectQueueSQLStatement: TSQLite3SQLStatement = //Select while ignoring or zeroing any wrong value
        '  WITH RECURSIVE Q AS ('
        + '    WITH IT AS ('
        + '      SELECT ID, Parent, Kind, Name, Name || IIF(Item.Kind = 1, ''/'', '''') AS Path FROM Item'
        + '        WHERE Kind IN (0, 1) AND Length(Name) <> 0 AND InStr(Name, ''/'') = 0)'
        + '    SELECT * FROM IT WHERE Parent = 0 UNION ALL'
        + '    SELECT IT.ID, IT.Parent, IT.Kind, IT.Name, Q.Path || IT.Path FROM IT'
        + '      INNER JOIN Q ON Q.Kind = 1 AND IT.Parent = Q.ID)'
        + '  SELECT IC.Content, IC.ContentPosition, Q.Kind, Q.Path, IC.ItemPosition, IC.Size FROM Q'
        + '  LEFT JOIN (SELECT Item, CAST(ItemPosition AS INTEGER) AS ItemPosition, Content,'
        + '               CAST(ContentPosition AS INTEGER) AS ContentPosition, CAST(Size AS INTEGER) AS Size FROM'
        + '               ItemContent WHERE CAST(ItemPosition AS INTEGER) >= 0 AND'
        + '               CAST(ContentPosition AS INTEGER) >= 0 AND CAST(Size AS INTEGER) > 0 AND'
        + '               Content IN (SELECT ID FROM Content)) AS IC ON Q.Kind = 0 AND Q.ID = IC.Item'
        + '  ORDER BY Content, ContentPosition, Path;';
    type
      TSelectQueueColumn = {%H-}(sqcContentID, sqcContentPosition, sqcKind, sqcPath, sqcItemPosition, sqcSize);

      function CheckSize(constref ACurrentContent: PQueueContent; AMaxPosition: Siz): Bool;
      begin
        Result := AMaxPosition = ACurrentContent^.TotalSize;
      end;

      function AddToQueue(AStatement: Psqlite3_stmt; var ACurrentContent: PQueueContent; var AMaxPosition: Siz): Bool;
      var
        CID: TSQLite3ROWID;
        IC: PQueueItemContent;
      begin
        Result := ColumnValue(AStatement, Ind(sqcContentID), CID, AStatus);
        if not Result then
          Exit;

        if (ACurrentContent = nil) or (CID <> ACurrentContent^.ID) then //Add a new content
        begin
          if ACurrentContent <> nil then
            CheckSize(ACurrentContent, AMaxPosition);
          ACurrentContent := AddEmptyPointer(AQueue.Contents);
          if ACurrentContent = nil then
            Exit(HandleError(pd0sNoMemory, AStatus));
          ACurrentContent^.ID := CID;
          AMaxPosition := 0;
        end;

        //Add ItemContent
        IC := AddEmptyPointer(ACurrentContent^.ItemContents);
        if IC = nil then
          Exit(HandleError(pd0sNoMemory, AStatus));

        Result := ColumnValue(AStatement, Ind(sqcKind), IC^.Kind, AStatus);
        Result := Result and ColumnValue(AStatement, Ind(sqcContentPosition), IC^.ContentPosition, AStatus);
        Result := Result and ColumnValue(AStatement, Ind(sqcPath), IC^.Path, AStatus);
        Result := Result and ColumnValue(AStatement, Ind(sqcItemPosition), IC^.FilePosition, AStatus);
        Result := Result and ColumnValue(AStatement, Ind(sqcSize), IC^.Size, AStatus);
        if not Result then
          Exit;

        ACurrentContent^.TotalSize += IC^.Size;
        AMaxPosition := Max(AMaxPosition, IC^.ContentPosition + IC^.Size);

        //Statistics
        AddTotalBytes(AExporter.Statistics, IC^.Size);
        AddItemCount(AExporter.Statistics, 1);

        Result := True;
      end;

    var
      STM: Psqlite3_stmt;
      R: TSQLite3ResultCode;
      CT: PQueueContent = nil;
      MP: Siz = 0;
    begin
      Result := Initialize(AConnection, SelectQueueSQLStatement, STM, AExporter.Error, AStatus);
      if Result then
      try
        while Step(STM, SQLITE_ROW, R) do
        begin
          Result := AddToQueue(STM, CT, MP);
          if (not Result) or CheckStop(AExporter, AStatus) then
            Exit(False);
        end;
        Result := CheckSQLite3Result(AExporter.Error, R, SQLITE_DONE, pd0sAbnormal, AStatus);
        if (not Result) or (CT = nil) then //If empty
          Exit;
        Result := CheckSize(CT, MP); //Check the last one if any
        if not Result then
          HandleError(AExporter.Error, pd0sInvalidItemContents, CT^.ID, AStatus);
      finally
        Finalize(STM);
      end;
    end;

  begin
    AQueue := Default(TQueue);
    Result := CheckExists and SelectQueue;
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
  if HandleExists and OpenConnection(CN) then
  try
    if HandleOptions(CN) and Prepare(CN, Q) and Process(CN, Q) then
      AStatus := pd0sDone;
  finally
    CloseConnection(CN);
  end;
end;

end.
