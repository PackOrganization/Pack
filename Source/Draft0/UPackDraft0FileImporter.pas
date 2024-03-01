unit UPackDraft0FileImporter;

{$I PackMode.inc}

interface

uses
  UFile, UPackDraft0Shared;

type
  TPackDraft0FileImporter = type TPackDraft0FileTaskHandler;
  PPackDraft0FileImporter = ^TPackDraft0FileImporter;

procedure Start(var AImporter: TPackDraft0FileImporter; constref AFile: TFile;
  AMode: TPackDraft0FileHandleMode; APress: TPackDraft0Press; const ASourcePaths: TFileSystemPathArray;
  out AStatus: TPackDraft0Status); overload;

implementation

uses
  UNumber, UString, UList, UNumberHelp, UMemoryBlock, UFileHelp, UFileHandleHelp, UFilePathHelp, UFileEnumerateHelp,
  UListHelp, UThread, USQLite3, USQLite3Help, UZstandard, UZstandardHelp,
  UPackDraft0SQLite3VFS;

type
  TQueueItem = record
    Parent: Ind; //Index in Queue.Items
    ParentPath: TDirectoryPath;
    Kind: TPackDraft0ItemKind;
    Name: TFileSystemName;
    Size: Siz;
    ID: TSQLite3ROWID;
  end;
  PQueueItem = ^TQueueItem;
  TQueueItemList = TList<TQueueItem>;

  TQueueItemContent = record
    Item: Ind; //Index in Queue.Items
    FilePosition: Ind;
    ContentPosition: Ind;
    Size: Siz;
  end;
  PQueueItemContent = ^TQueueItemContent;
  TQueueItemContentList = TList<TQueueItemContent>;

  TQueueContent = record
    ID: TSQLite3ROWID;
    ItemContents: TQueueItemContentList;
  end;
  PQueueContent = ^TQueueContent;
  TQueueContentList = TList<TQueueContent>;

  TQueue = record
    Items: TQueueItemList;
    Contents: TQueueContentList;
    LastPickedContent: Ind;
  end;

  TProcessorsContext = object(TPackDraft0FileTaskHandlerProcessorsContext<TQueue>)
    Press: TPackDraft0Press;
    BundleSize: Siz;
    class procedure Act(var AContext: TProcessorsContext; var AError: TPackDraft0Error;
      out AStatus: TPackDraft0Status); static;
  end;
  PProcessorsContext = ^TProcessorsContext;

class procedure TProcessorsContext.Act(var AContext: TProcessorsContext; var AError: TPackDraft0Error;
  out AStatus: TPackDraft0Status);

  function Initialize(out AConverter: PZSTD_CCtx; out AContent, AConvertedContent: TMemoryBlock;
    out AInsertContentStatement: Psqlite3_stmt): Bool; overload;

    function InitializeConverter(out ANeededSize: Siz): Bool; overload;
    begin
      case AContext.Press of
        pd0pNone: Result := Create(AConverter, TZSTDStrategy(ZSTD_dfast),
            EffectiveZSTDWindowLog(AContext.BundleSize),
            16, 17, 1, 5, 0);
        pd0pHard: Result := Create(AConverter, TZSTDStrategy(ZSTD_btopt),
            EffectiveZSTDWindowLog(AContext.BundleSize),
            22, 22, 5, 5, 48);
      end;
      ANeededSize := ZSTD_compressBound(AContext.BundleSize);
      if not Result then
        HandleError(pd0sCanNotInitializeConverter, AStatus);
    end;

  const
    InsertContentSQLStatement: TSQLite3SQLStatement = 'INSERT INTO Content(ID, Value) VALUES(?, ?);';
  var
    NS: Siz;
  begin
    AConverter := nil;
    AInsertContentStatement := nil;
    AContent := Default(TMemoryBlock);
    AConvertedContent := Default(TMemoryBlock);
    try
      Result := InitializeConverter(NS);
      Result := Result and Initialize(AContext.Connection, InsertContentSQLStatement, AInsertContentStatement,
        AError, AStatus);
      Result := Result and Capacity(AContent, AContext.BundleSize, AStatus);
      Result := Result and Capacity(AConvertedContent, NS, AStatus);
    finally
      if not Result then
      begin
        ZSTD_freeCCtx(AConverter);
        Finalize(AInsertContentStatement);
        Clear(AContent);
        Clear(AConvertedContent);
      end;
    end;
  end;

  procedure Finalize(var AConverter: PZSTD_CCtx; var AContent, AConvertedContent: TMemoryBlock;
  var AInsertContentStatement: Psqlite3_stmt); overload;
  begin
    Clear(AContent);
    Clear(AConvertedContent);
    Finalize(AInsertContentStatement);
    ZSTD_freeCCtx(AConverter);
  end;

  function Read(constref AQueueContent: TQueueContent; var AContent: TMemoryBlock): Bool; overload;

    function ReadItemContent(const AItemContent: TQueueItemContent): Bool; overload;
    var
      IT: PQueueItem;
      P: TFileSystemPath;
      H: TFileHandler;
    begin
      Result := False;
      if CheckStopContext(AContext, AStatus) then
        Exit(False);
      IT := ItemPointer(AContext.Queue^.Items, AItemContent.Item);
      P := IT^.ParentPath + It^.Name;
      if not Open(&File(P), [ofoRead, ofoShareRead, ofoShareWrite, ofoShareDelete], H) then
        Exit(HandleOSError(AError, pd0sCanNotOpenFile, P, AStatus));
      try
        if AItemContent.FilePosition <> 0 then
          if not SeekTo(H, AItemContent.FilePosition) then
            Exit(HandleOSError(AError, pd0sCanNotSeekFile, P, AStatus));
        if not Read(H, Data(AContent) + AItemContent.ContentPosition, AItemContent.Size) then
          Exit(HandleOSError(AError, pd0sCanNotReadFile, P, AStatus));
        Result := True;
      finally
        Close(H);
      end;
    end;

  var
    I: Ind;
  begin
    Result := True;
    with AQueueContent do
      for I := First(ItemContents) to Last(ItemContents) do
        if not ReadItemContent(PQueueItemContent(ItemPointer(ItemContents, I))^) then
          Exit(False);
    Size(AContent, Size<TQueueContent, TQueueItemContent>(AQueueContent));
  end;

  function Convert(constref AQueueContent: TQueueContent; AConverter: PZSTD_CCtx; constref AContent: TMemoryBlock;
  var AConvertedContent: TMemoryBlock; out AResultContent: TMemoryBlock): Bool; overload;
  const
    Tolerance: Siz = 4 * 1024;
  var
    R: size_t;
    C: Bool;
  begin
    R := ZSTD_compress2(AConverter, Data(AConvertedContent),
      Capacity(AConvertedContent), Data(AContent), Size(AContent));
    if ZSTD_isError(R) = 1 then
      Exit(HandleError(AError, pd0sCanNotConvert, AQueueContent.ID, AStatus));
    Size(AConvertedContent, R);

    //Check if it worth it
    C := (Size(AContent) > Tolerance) and //Not very small
      (Size(AConvertedContent) < (Size(AContent) - Tolerance)); //Effective compression
    AResultContent := Condition(C, AConvertedContent, AContent);
    Result := True;
  end;

  function Write(AStatement: Psqlite3_stmt; constref AQueueContent: TQueueContent;
    constref AContent: TMemoryBlock): Bool; overload;
  var
    R: TSQLite3ResultCode;
  begin
    Insert(AStatement, AQueueContent.ID, AContent, R);
    Result := CheckSQLite3Result(AError, R, pd0sCanNotUseStatement, AStatus);
  end;

var
  CV: PZSTD_CCtx;
  C, CC: TMemoryBlock;
  SM: Psqlite3_stmt;
  QC: PQueueContent;
  RC: TMemoryBlock;
begin
  AStatus := pd0sUnknown;
  if Initialize(CV, C, CC, SM) then
  try
    while (Next(AContext.Queue^, QC, AStatus)) and Read(QC^, C) and Convert(QC^, CV, C, CC, RC) and Write(SM, QC^, RC) do
      AddDoneBytes(AContext.Statistics^, Size(C)); //Statistics
  finally
    Finalize(CV, C, CC, SM);
  end;
end;

procedure ThreadGroupMethod(AIndex: Ind; AContext: Ptr); overload;
begin
  UPackDraft0Shared.ThreadGroupMethod<TProcessorsContext>(AIndex, PProcessorsContext(AContext)^);
end;

procedure Start(var AImporter: TPackDraft0FileImporter; constref AFile: TFile; AMode: TPackDraft0FileHandleMode;
  APress: TPackDraft0Press; const ASourcePaths: TFileSystemPathArray; out AStatus: TPackDraft0Status);

  function HandleExists(out AFileExists: Bool): Bool;
  begin
    AFileExists := Exists(AsFileSystemObject(AFile));
    case AMode of
      pd0fhmCreate:
        if AFileExists then
          Exit(HandleError(AImporter.Error, pd0sAlreadyExists, '', AStatus));
      pd0fhmCreateOrOverwrite:
        if AFileExists then
          if Destroy(AFile) then
            AFileExists := False
          else
            Exit(HandleOSError(AImporter.Error, pd0sCanNotOverwrite, '', AStatus));
    end;
    Result := True;
  end;

  function Scan(var AItems: TQueueItemList; out ATotalSize: Siz): Bool;

    function Add(AParent: Ind; const AParentPath: TDirectoryPath; const AName: TFileSystemName;
      AKind: TFileSystemObjectKind; ASize: Siz): Bool; overload; forward;

    function ScanFolder(AItem: Ind; const APath: TDirectoryPath): Bool;
    var
      E: TDirectoryEnumerator;
    begin
      Result := Open(Directory(APath), E);
      if not Result then
        Exit(HandleOSError(AImporter.Error, pd0sCanNotOpenFolder, APath, AStatus));
      try
        while Result and Next(E) do
          Result := Add(AItem, APath, Name(E, False), Kind(E), Size(E));
      finally
        Close(E);
      end;
    end;

    //Add Items to the queue to process later
    function Add(AParent: Ind; const AParentPath: TDirectoryPath; const AName: TFileSystemName;
      AKind: TFileSystemObjectKind; ASize: Siz): Bool; overload;

      function AddToQueue(AKind: TPackDraft0ItemKind; out AIndex: Ind): Bool;
      var
        IT: PQueueItem;
      begin
        AIndex := AddEmpty(AItems, IT);
        if IT = nil then
          Exit(HandleError(pd0sNoMemory, AStatus));
        with IT^ do
        begin
          Parent := AParent;
          ParentPath := AParentPath;
          Kind := AKind;
          Name := AName;
          if Kind = pd0ikFolder then
            SetLength(Name, Length(Name) - 1); //Removing PathDelimiter
          Size := ASize;
        end;

        ATotalSize += ASize;

        //Statistics
        AddTotalBytes(AImporter.Statistics, ASize);
        AddItemCount(AImporter.Statistics, 1);

        Result := True;
      end;

    var
      K: TPackDraft0ItemKind;
      ITI: Ind;
    begin
      if CheckStop(AImporter, AStatus) then
        Exit(False);
      K := ToItemKind(AKind);
      if not (K in [pd0ikFolder, pd0ikFile]) then
        Exit(HandleError(AImporter.Error, pd0sNotSupportedPath, AParentPath + AName, AStatus));
      Result := AddToQueue(K, ITI);
      if Result and (K = pd0ikFolder) then
        Result := ScanFolder(ITI, AParentPath + AName);
    end;

    function ScanPath(const APath: TFileSystemPath): Bool;
    var
      A: TFileSystemObjectAttributesHandler;
      P: TDirectoryPath;
      N: TFileSystemName;
    begin
      Result := OpenLoad(FileSystemObject(APath), A);
      if not Result then
        Exit(HandleOSError(AImporter.Error, pd0sCanNotReadFileAttributes, APath, AStatus));
      try
        Split(APath, [], P, N);
        Result := Add(InvalidIndex, P, N, Kind(A), Size(A));
      finally
        Close(A);
      end;
    end;

  var
    I: Ind;
  begin
    ATotalSize := 0;
    for I := 0 to High(ASourcePaths) do
      if not ScanPath(ASourcePaths[I]) then
        Exit(False);
    Result := True; //Even for empty
  end;

  function EffectiveBundleSize(constref AQueue: TQueue): Siz;
  begin
    case APress of
      pd0pNone: Result := Condition(Count(AQueue.Items) >= 1000, //A good looking big number
          16, 8);
      pd0pHard: Result := 32;
    end;
    Result := Result * 1024 * 1024;
  end;

  //Split Items into fixed size Contents, and each part will be an ItemContent
  function Split(var AQueue: TQueue; ABundleSize: Siz): Bool;
  var
    LCID: TSQLite3ROWID;
    ITI, CP, ITP: Ind;
    CS, ITS, ICS: Siz;
    IT: PQueueItem;
    CT: PQueueContent;
    IC: PQueueItemContent;
  begin
    LCID := 0; //Last Content ID. Todo: Query form Database on Append
    CS := 0;
    CP := 0;
    CT := nil;

    //Loop over all Items and split them into Contents
    for ITI := First(AQueue.Items) to Last(AQueue.Items) do
    begin
      //Pick an Item
      IT := ItemPointer(AQueue.Items, ITI);
      ITS := IT^.Size;
      if ITS = 0 then
        Continue; //No Content and ItemContent for folders and empty files
      ITP := 0;

      //Split it
      repeat
        if CP = CS then //Add a new content
        begin
          CT := AddEmptyPointer(AQueue.Contents);
          if CT = nil then
            Exit(HandleError(pd0sNoMemory, AStatus));
          LCID += 1; //New Content ID
          CT^.ID := LCID;
          CS := ABundleSize;
          CP := 0;
        end;

        //Choose ItemContent Size
        ICS := ITS - ITP;
        if ICS > (CS - CP) then //Prevent going over the size of Content
          ICS := CS - CP;

        //Add ItemContent
        IC := AddEmptyPointer(CT^.ItemContents);
        if IC = nil then
          Exit(HandleError(pd0sNoMemory, AStatus));
        IC^.Item := ITI;
        IC^.FilePosition := ITP;
        IC^.ContentPosition := CP;
        IC^.Size := ICS;

        //Increment Item Position and Content Position
        CP += ICS;
        ITP += ICS;
      until ITP = ITS;
    end;
    Result := True;
  end;

  function Prepare(out AQueue: TQueue; out ATotalSize, ABundleSize: Siz): Bool; overload;
  begin
    AQueue := Default(TQueue);
    Result := Scan(AQueue.Items, ATotalSize);
    ABundleSize := EffectiveBundleSize(AQueue);
    Result := Result and Split(AQueue, ABundleSize);
    AQueue.LastPickedContent := -1;
  end;

  function OpenConnection(out AConnection: Psqlite3): Bool;
  var
    R: TSQLite3ResultCode;
  begin
    R := Open(Path(AFile), SQLITE_OPEN_CREATE or SQLITE_OPEN_READWRITE or SQLITE_OPEN_FULLMUTEX or
      SQLITE_OPEN_EXRESCODE, AConnection);
    Result := CheckSQLite3Result(AImporter.Error, R, pd0sCanNotOpen, AStatus);
  end;

  function CloseConnection(AConnection: Psqlite3; ACancel: Bool; AFileExists: Bool): Bool;
  var
    R: TSQLite3ResultCode;
  begin
    R := Close(AConnection);
    Result := CheckSQLite3Result(AImporter.Error, R, pd0sCanNotClose, AStatus);
    if not Result then
      Exit;

    if ACancel and (not AFileExists) then //Delete if there is an issue and the file is new
      if not Destroy(AFile) then
        HandleOSError(AImporter.Error, pd0sAbnormal, '', AStatus);
  end;

  function HandleOptions(AConnection: Psqlite3; ATotalSize: Siz; AFileExists: Bool): Bool;

    function EffectivePageSize: Siz;
    begin
      Result := Condition(ATotalSize >= 1024 * 1024, //A good number for big
        4096, 512);
    end;

  begin
    Result := EnableSecurity(AConnection, AStatus);
    Result := Result and Options(AConnection, s3smOff, s3lmExclusive, s3jmOff, s3tmMemory,
      Condition<TSQLite3PageSize>(AFileExists, 0, EffectivePageSize), //Change if it is a new file
      AImporter.Error, AStatus);
  end;

  function BeginUpdate(AConnection: Psqlite3): Bool; overload;
  var
    R: TSQLite3ResultCode;
  begin
    R := USQLite3Help.BeginTransaction(AConnection);
    Result := CheckSQLite3Result(AImporter.Error, R, pd0sCanNotProcessTransaction, AStatus);
  end;

  function EndUpdate(AConnection: Psqlite3; ACancel: Bool): Bool; overload;
  var
    R: TSQLite3ResultCode;
  begin
    R := USQLite3Help.EndTransaction(AConnection, ACancel);
    Result := CheckSQLite3Result(AImporter.Error, R, pd0sCanNotProcessTransaction, AStatus);
  end;

  function HandleTables(AConnection: Psqlite3; AFileExists: Bool): Bool;
  const
    CreateItemTableSQLStatement: TSQLite3SQLStatement =
      'CREATE TABLE Item(ID INTEGER PRIMARY KEY, Parent INTEGER, Kind INTEGER, Name TEXT);';
    CreateContentTableSQLStatement: TSQLite3SQLStatement = 'CREATE TABLE Content(ID INTEGER PRIMARY KEY, Value BLOB);';
    CreateItemContentTableSQLStatement: TSQLite3SQLStatement =
      'CREATE TABLE ItemContent(ID INTEGER PRIMARY KEY, Item INTEGER, ItemPosition INTEGER, Content INTEGER, ContentPosition INTEGER, Size INTEGER);';
  var
    R: TSQLite3ResultCode;
  begin
    if AFileExists then //Create Tables if it is a new file
      Exit(True); //It must have the correct tables
    Execute(AConnection, [CreateItemTableSQLStatement, CreateContentTableSQLStatement,
      CreateItemContentTableSQLStatement], False, R);
    Result := CheckSQLite3Result(AImporter.Error, R, pd0sAbnormal, AStatus);
  end;

  function Insert(AConnection: Psqlite3; var AQueue: TQueue): Bool;

    function InsertItems: Bool;
    const
      InsertItemSQLStatement: TSQLite3SQLStatement = 'INSERT INTO Item(Parent, Kind, Name) VALUES(?, ?, ?);';
    var
      STM: Psqlite3_stmt;
      I: Ind;
      PD: TSQLite3ROWID;
      R: TSQLite3ResultCode;
    begin
      Result := Initialize(AConnection, InsertItemSQLStatement, STM, AImporter.Error, AStatus);
      if Result then
      try
        for I := First(AQueue.Items) to Last(AQueue.Items) do
          with PQueueItem(ItemPointer(AQueue.Items, I))^ do
          begin
            if Parent <> InvalidIndex then
              PD := PQueueItem(ItemPointer(AQueue.Items, Parent))^.ID
            else
              PD := 0;
            Result := USQLite3Help.Insert(STM, PD, Kind, Name, R);
            Result := CheckSQLite3Result(AImporter.Error, R, pd0sCanNotUseStatement, AStatus);
            ID := InsertedID(AConnection);
            if (not Result) or CheckStop(AImporter, AStatus) then
              Exit(False);
          end;
      finally
        Finalize(STM);
      end;
    end;

    function InsertContentItems: Bool;
    const
      InsertItemContentSQLStatement: TSQLite3SQLStatement =
        'INSERT INTO ItemContent(Item, ItemPosition, Content, ContentPosition, Size) VALUES(?, ?, ?, ?, ?);';
    var
      STM: Psqlite3_stmt;
      I, J: Ind;
      R: TSQLite3ResultCode;
    begin
      Result := Initialize(AConnection, InsertItemContentSQLStatement, STM, AImporter.Error, AStatus);
      if Result then
      try
        for I := First(AQueue.Contents) to Last(AQueue.Contents) do
          with PQueueContent(ItemPointer(AQueue.Contents, I))^ do
            for J := First(ItemContents) to Last(ItemContents) do
              with PQueueItemContent(ItemPointer(ItemContents, J))^ do
              begin
                Result := USQLite3Help.Insert(STM, PQueueItem(ItemPointer(AQueue.Items, Item))^.ID,
                  FilePosition, ID, ContentPosition, Size, R);
                Result := CheckSQLite3Result(AImporter.Error, R, pd0sCanNotUseStatement, AStatus);
                if (not Result) or CheckStop(AImporter, AStatus) then
                  Exit(False);
              end;
      finally
        Finalize(STM);
      end;
    end;

  begin
    Result := InsertItems and InsertContentItems;
  end;

  function Process(AConnection: Psqlite3; constref AQueue: TQueue; ABundleSize: Siz): Bool; overload;
  var
    CX: TProcessorsContext;
  begin
    CX := Default(TProcessorsContext);
    CX.BundleSize := ABundleSize;
    CX.Press := APress;
    Result := UPackDraft0Shared.Process<TQueue>(AImporter, CX, AConnection, ThreadGroupMethod, AQueue, AStatus);
  end;

var
  EX: Bool;
  Q: TQueue;
  TS, BS: Siz;
  CN: Psqlite3;
begin
  AStatus := pd0sUnknown;
  if HandleExists(EX) and Prepare(Q, TS, BS) and OpenConnection(CN) then
  try
    if HandleOptions(CN, TS, EX) and BeginUpdate(CN) then
    try
      if HandleTables(CN, EX) and Insert(CN, Q) and Process(CN, Q, BS) then
        AStatus := pd0sDone;
    finally
      EndUpdate(CN, AStatus <> pd0sDone);
    end;
  finally
    CloseConnection(CN, AStatus <> pd0sDone, EX);
  end;
end;

end.
