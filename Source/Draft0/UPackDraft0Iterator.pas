unit UPackDraft0Iterator;

{$I PackMode.inc}

interface

uses
  UNumber, UFile, UList, UListHelp, USQLite3, USQLite3Help,
  UPackDraft0Shared;

type
  TQueueItem = record
  private
    ParentIndex: Ind; //Index in Queue.Items
    ID: TSQLite3ROWID;
    Parent: TSQLite3ROWID;
    Kind: TPackDraft0ItemKind;
    Name: TPackDraft0ItemName;
    Size: Siz;
  end;
  PQueueItem = ^TQueueItem;

  TQueue = record
  private
    Items: TList<TQueueItem>;
  end;

  TPackDraft0Iterator = object(TPackDraft0TaskHandler)
  private
    Queue: TQueue;
    CurrentIndex: Ind;
    Current: PQueueItem;
  end;

procedure Open(constref AFile: TFile; out AIterator: TPackDraft0Iterator; const AIncludeIDs: TPackDraft0ItemIDArray;
  const AIncludePaths: TPackDraft0ItemPathArray; out AStatus: TPackDraft0Status); overload;
procedure Close(var AIterator: TPackDraft0Iterator; out AStatus: TPackDraft0Status); overload;
function Next(var AIterator: TPackDraft0Iterator): Bool; overload;
function ID(constref AIterator: TPackDraft0Iterator): TPackDraft0ItemID; inline; overload;
function Parent(constref AIterator: TPackDraft0Iterator): TPackDraft0ItemID; inline; overload;
function Kind(constref AIterator: TPackDraft0Iterator): TPackDraft0ItemKind; inline; overload;
function Name(constref AIterator: TPackDraft0Iterator): TPackDraft0ItemName; inline; overload;
function Size(constref AIterator: TPackDraft0Iterator): Siz; inline; overload;
function Path(constref AIterator: TPackDraft0Iterator): TPackDraft0ItemPath; inline; overload;

implementation

procedure Open(constref AFile: TFile; out AIterator: TPackDraft0Iterator; const AIncludeIDs: TPackDraft0ItemIDArray;
  const AIncludePaths: TPackDraft0ItemPathArray; out AStatus: TPackDraft0Status);

  function Prepare(AConnection: Psqlite3; out AQueue: TQueue): Bool;

    function SelectItems: Bool;
    var
      IIS, SQ: TSQLite3SQLStatement;
    var
      STM: Psqlite3_stmt;
      R: TSQLite3ResultCode;
      IT: PQueueItem;
    begin
      IIS := IndexedItemsSQLite3SQLStatement(AIncludeIDs, AIncludePaths);
      SQ := 'SELECT IT.PI, IT.ID, Parent, Kind, Name, TOTAL(Size) AS Size FROM (' + IIS +
        ') AS IT LEFT JOIN ItemContent ON IT.ID = ItemContent.Item GROUP BY IT.I;';
      Result := Initialize(AConnection, SQ, STM, AIterator.Error, AStatus);
      if Result then
      try
        while Step(STM, SQLITE_ROW, R) do
        begin
          if not (AddEmpty<TQueueItem>(AQueue.Items, IT, AStatus)) then //Add Item
            Exit(False);
          Result := ColumnAll<Ind, TSQLite3ROWID, TSQLite3ROWID, TPackDraft0ItemKind,
            TPackDraft0ItemName, Siz>(
            STM, IT^.ParentIndex, IT^.ID, IT^.Parent, IT^.Kind, IT^.Name, IT^.Size, AStatus);
          if (not Result) or CheckStop(AIterator, AStatus) then
            Exit(False);
        end;
        Result := CheckSQLite3Result(AIterator.Error, R, SQLITE_DONE, pd0sAbnormal, AStatus);
      finally
        Finalize(STM);
      end;
    end;

  begin
    AQueue := Default(TQueue);
    Result := SelectItems;
  end;

var
  CN: Psqlite3;
begin
  AStatus := pd0sUnknown;
  AIterator := Default(TPackDraft0Iterator);
  with AIterator do
    if OpenConnection(AFile, True, CN, Error, AStatus) then
    try
      if HandleOptions(CN, 0, Error, AStatus) and Prepare(CN, Queue) then
        AStatus := pd0sDone;
    finally
      if AStatus <> pd0sDone then
        Queue := Default(TQueue);
      CloseConnection(CN, Error, AStatus);
    end;
end;

procedure Close(var AIterator: TPackDraft0Iterator; out AStatus: TPackDraft0Status);
begin
  AIterator := Default(TPackDraft0Iterator);
  AStatus := pd0sDone;
end;

function Next(var AIterator: TPackDraft0Iterator): Bool;
begin
  Result := AIterator.CurrentIndex <= Last(AIterator.Queue.Items);
  if not Result then
    Exit;
  AIterator.Current := ItemPointer(AIterator.Queue.Items, AIterator.CurrentIndex);
  AIterator.CurrentIndex += 1;
end;

function ID(constref AIterator: TPackDraft0Iterator): TPackDraft0ItemID;
begin
  Result := AIterator.Current^.ID;
end;

function Parent(constref AIterator: TPackDraft0Iterator): TPackDraft0ItemID;
begin
  Result := AIterator.Current^.Parent;
end;

function Kind(constref AIterator: TPackDraft0Iterator): TPackDraft0ItemKind;
begin
  Result := AIterator.Current^.Kind;
end;

function Name(constref AIterator: TPackDraft0Iterator): TPackDraft0ItemName;
begin
  Result := AIterator.Current^.Name;
end;

function Size(constref AIterator: TPackDraft0Iterator): Siz;
begin
  Result := AIterator.Current^.Size;
end;

function Path(constref AIterator: TPackDraft0Iterator): TPackDraft0ItemPath;
begin
  Result := ItemFileSystemPath<TQueueItem>(PathDelimiter, AIterator.Current^, AIterator.Queue.Items, True);
end;

end.
