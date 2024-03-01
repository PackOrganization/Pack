unit UPackDraft0SQLite3VFS;

{$I PackMode.inc}

interface

implementation

uses
  UNumber, UString, UMemory, UMemoryHelp, USQLite3,
  UPackDraft0Shared;

type
  TVFSFile = object(sqlite3_file)
    OpenFlags: int;
  end;
  PVFSFile = ^TVFSFile;

function Main(pVfs: Psqlite3_vfs): Psqlite3_vfs; inline; overload;
begin
  Result := Psqlite3_vfs(pVfs^.pAppData);
end;

function Main(constref pfile: Psqlite3_file): Psqlite3_file; inline; overload;
begin
  Result := PVFSFile(pfile) + 1; //The Main VFS file is after This VFS file
end;

function Methods(pfile: Psqlite3_file): Psqlite3_io_methods; inline; overload;
begin
  Result := Main(pfile)^.pMethods;
end;

function Close(pFile: Psqlite3_file): int; cdecl;
begin
  Result := Methods(pFile)^.xClose(Main(pFile));
end;

function Read(pFile: Psqlite3_file; zBuf: Ptr; iAmt: int; iOfst: sqlite3_int64): int; cdecl;
begin
  Result := Methods(pFile)^.xRead(Main(pFile), zBuf, iAmt, iOfst);

  //Check
  if (Result = SQLITE_OK) and ((PVFSFile(pFile)^.OpenFlags and SQLITE_OPEN_MAIN_DB) <> 0) and (iOfst = 0) then
  begin
    if CheckHeader(zBuf, iAmt) then
      WriteSQLite3Header(zBuf)
    else
      Result := SQLITE_NOTADB;
  end;
end;

function Write(pFile: Psqlite3_file; const zBuf: Ptr; iAmt: int; iOfst: sqlite3_int64): int; cdecl;
var
  P: Ptr;
begin
  //Change Header
  if ((PVFSFile(pFile)^.OpenFlags and SQLITE_OPEN_MAIN_DB) <> 0) and (iOfst = 0) then
  begin
    if iAmt < 16 then //At least 16 bytes for a valid header
      Exit(SQLITE_NOTADB);

    P := Copy(zBuf, iAmt);
    if P = nil then
      Exit(SQLITE_NOMEM);
    try
      WritePackHeader(P);
      Result := Methods(pFile)^.xWrite(Main(pFile), P, iAmt, iOfst);
    finally
      Deallocate(P);
    end;
  end
  else
    Result := Methods(pFile)^.xWrite(Main(pFile), zBuf, iAmt, iOfst);
end;

function Truncate(pFile: Psqlite3_file; size: sqlite3_int64): int; cdecl;
begin
  Result := Methods(pFile)^.xTruncate(Main(pFile), size);
end;

function Sync(pFile: Psqlite3_file; flags: int): int; cdecl;
begin
  Result := Methods(pFile)^.xSync(Main(pFile), flags);
end;

function FileSize(pFile: Psqlite3_file; pSize: Psqlite3_int64): int; cdecl;
begin
  Result := Methods(pFile)^.xFileSize(Main(pFile), pSize);
end;

function Lock(pFile: Psqlite3_file; eLock: int): int; cdecl;
begin
  Result := Methods(pFile)^.xLock(Main(pFile), eLock);
end;

function Unlock(pFile: Psqlite3_file; eLock: int): int; cdecl;
begin
  Result := Methods(pFile)^.xUnlock(Main(pFile), eLock);
end;

function CheckReservedLock(pFile: Psqlite3_file; pResOut: Pint): int; cdecl;
begin
  Result := Methods(pFile)^.xCheckReservedLock(Main(pFile), pResOut);
end;

function FileControl(pFile: Psqlite3_file; op: int; pArg: Ptr): int; cdecl;
begin
  Result := Methods(pFile)^.xFileControl(Main(pFile), op, pArg);
end;

function SectorSize(pFile: Psqlite3_file): int; cdecl;
begin
  Result := Methods(pFile)^.xSectorSize(Main(pFile));
end;

function DeviceCharacteristics(pFile: Psqlite3_file): int; cdecl;
begin
  Result := Methods(pFile)^.xDeviceCharacteristics(Main(pFile));
end;

function ShmMap(pFile: Psqlite3_file; iPg: int; pgsz: int; isWrite: int; volatile: PPtr): int; cdecl;
begin
  Result := Methods(pFile)^.xShmMap(Main(pFile), iPg, pgsz, isWrite, volatile);
end;

function ShmLock(pFile: Psqlite3_file; offset: int; n: int; flags: int): int; cdecl;
begin
  Result := Methods(pFile)^.xShmLock(Main(pFile), offset, n, flags);
end;

procedure ShmBarrier(pFile: Psqlite3_file); cdecl;
begin
  Methods(pFile)^.xShmBarrier(Main(pFile));
end;

function ShmUnmap(pFile: Psqlite3_file; deleteFlag: int): int; cdecl;
begin
  Result := Methods(pFile)^.xShmUnmap(Main(pFile), deleteFlag);
end;

function Fetch(pFile: Psqlite3_file; iOfst: sqlite3_int64; iAmt: int; pp: PPtr): int; cdecl;
begin
  Result := Methods(pFile)^.xFetch(Main(pFile), iOfst, iAmt, pp);
end;

function Unfetch(pFile: Psqlite3_file; iOfst: sqlite3_int64; iAmt: int; p: Ptr): int; cdecl;
begin
  Result := Methods(pFile)^.xUnfetch(Main(pFile), iOfst, iAmt, p);
end;

const
  FileMethods: sqlite3_io_methods = (
    iVersion: 3;
    xClose: @Close;
    xRead: @Read;
    xWrite: @Write;
    xTruncate: @Truncate;
    xSync: @Sync;
    xFileSize: @FileSize;
    xLock: @Lock;
    xUnlock: @Unlock;
    xCheckReservedLock: @CheckReservedLock;
    xFileControl: @FileControl;
    xSectorSize: @SectorSize;
    xDeviceCharacteristics: @DeviceCharacteristics;
    xShmMap: @ShmMap;
    xShmLock: @ShmLock;
    xShmBarrier: @ShmBarrier;
    xShmUnmap: @ShmUnmap;
    xFetch: @Fetch;
    xUnfetch: @Unfetch; );

function Open(pVfs: Psqlite3_vfs; zName: sqlite3_filename; pfile: Psqlite3_file; flags: int;
  pOutFlags: Pint): int; cdecl;
begin
  Result := Main(pVfs)^.xOpen(Main(pVfs), zName, Main(pfile), flags, pOutFlags);
  if Result <> SQLITE_OK then
    Exit;
  pfile^.pMethods := @FileMethods;
  PVFSFile(pfile)^.OpenFlags := flags; //To check later in read and write
end;

function Access(pVfs: Psqlite3_vfs; const zName: PChar; flags: int; pResOut: Pint): int; cdecl;
begin
  Result := Main(pVfs)^.xAccess(Main(pVfs), zName, flags, pResOut);
end;

function Delete(pVfs: Psqlite3_vfs; const zName: PChar; syncDir: int): int; cdecl;
begin
  Result := Main(pVfs)^.xDelete(Main(pVfs), zName, syncDir);
end;

function FullPathname(pVfs: Psqlite3_vfs; const zName: PChar; nOut: int; zOut: PChar): int; cdecl;
begin
  Result := Main(pVfs)^.xFullPathname(Main(pVfs), zName, nOut, zOut);
end;

function DlOpen(pVfs: Psqlite3_vfs; const zFilename: PChar): Ptr; cdecl;
begin
  Result := Main(pVfs)^.xDlOpen(Main(pVfs), zFilename);
end;

procedure DlError(pVfs: Psqlite3_vfs; nByte: int; zErrMsg: PChar); cdecl;
begin
  Main(pVfs)^.xDlError(Main(pVfs), nByte, zErrMsg);
end;

function DlSym(pVfs: Psqlite3_vfs; pHandle: Ptr; const zSymbol: PChar): Ptr; cdecl;
begin
  Result := Main(pVfs)^.xDlSym(Main(pVfs), pHandle, zSymbol);
end;

procedure DlClose(pVfs: Psqlite3_vfs; pHandle: Ptr); cdecl;
begin
  Main(pVfs)^.xDlClose(Main(pVfs), pHandle);
end;

function Randomness(pVfs: Psqlite3_vfs; nByte: int; zOut: PChar): int; cdecl;
begin
  Result := Main(pVfs)^.xRandomness(Main(pVfs), nByte, zOut);
end;

function Sleep(pVfs: Psqlite3_vfs; microseconds: int): int; cdecl;
begin
  Result := Main(pVfs)^.xSleep(Main(pVfs), microseconds);
end;

function CurrentTime(pVfs: Psqlite3_vfs; pTimeOut: PDouble): int; cdecl;
begin
  Result := Main(pVfs)^.xCurrentTime(Main(pVfs), pTimeOut);
end;

function GetLastError(pVfs: Psqlite3_vfs; iErr: int; zErr: PChar): int; cdecl;
begin
  Result := Main(pVfs)^.xGetLastError(Main(pVfs), iErr, zErr);
end;

function CurrentTimeInt64(pVfs: Psqlite3_vfs; pTimeOut: Psqlite3_int64): int; cdecl;
begin
  Result := Main(pVfs)^.xCurrentTimeInt64(Main(pVfs), pTimeOut);
end;

function SetSystemCall(pVfs: Psqlite3_vfs; const zName: PChar; v: sqlite3_syscall_ptr): int; cdecl;
begin
  Result := Main(pVfs)^.xSetSystemCall(Main(pVfs), zName, v);
end;

function GetSystemCall(pVfs: Psqlite3_vfs; const zName: PChar): sqlite3_syscall_ptr; cdecl;
begin
  Result := Main(pVfs)^.xGetSystemCall(Main(pVfs), zName);
end;

function NextSystemCall(pVfs: Psqlite3_vfs; const zName: PChar): PChar; cdecl;
begin
  Result := Main(pVfs)^.xNextSystemCall(Main(pVfs), zName);
end;

procedure Create(out AVFS: sqlite3_vfs; AMainVFS: Psqlite3_vfs);
begin
  AVFS := Default(sqlite3_vfs);
  with AVFS do
  begin
    iVersion := AMainVFS^.iVersion;
    szOsFile := AMainVFS^.szOsFile + SizeOf(TVFSFile);
    mxPathname := AMainVFS^.mxPathname;
    zName := 'PackVFS';
    pAppData := AMainVFS;
    xOpen := @Open;
    xDelete := @Delete;
    xAccess := @Access;
    xFullPathname := @FullPathname;
    xDlOpen := @DlOpen;
    xDlError := @DlError;
    xDlSym := @DlSym;
    xDlClose := @DlClose;
    xRandomness := @Randomness;
    xSleep := @Sleep;
    xCurrentTime := @CurrentTime;
    xGetLastError := @GetLastError;
    xCurrentTimeInt64 := @CurrentTimeInt64;
    xSetSystemCall := @SetSystemCall;
    xGetSystemCall := @GetSystemCall;
    xNextSystemCall := @NextSystemCall;
  end;
end;

var
  DV: Psqlite3_vfs;
  NV: sqlite3_vfs;

initialization
  DV := sqlite3_vfs_find(nil);
  Create(NV, DV);
  sqlite3_vfs_register(@NV, 1);

finalization
  sqlite3_vfs_unregister(@NV);
end.
