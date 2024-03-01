unit UMain;

{$I PackMode.inc}

{$Define OtherOptions}//Intended for Draft releases

interface

uses
  UNumber;

function Run: Bool; overload;
function Stop: Bool; overload;

implementation

uses
  UString, UFile, UNumberHelp, UStringHelp, UFileHelp, UFilePathHelp, UArrayHelp, UProgramCommandLine,
  UThread, UThreadHelp, USystem, UTick, USQLite3Help, UFileCompare, UByteUnit, UByteUnitHelp,
  UPackProgramShared,
  UPackDraft0Shared, UPackDraft0FileImporter, UPackDraft0FileExporter;

type
  TProgramTaskKind = (ptkNone, ptkPack, ptkUnpack, ptkList, ptkTransformToSQLite3, ptkTransformToPack);
  TParameterKind = (pkUnknown, pkHelp, pkPack, pkList, pkInput, pkOutput, pkLog, pkOverwrite, pkPress
    {$IfDef OtherOptions}, pkActivateOtherOptions, pkVerifyPack, pkTransformToSQLite3, pkTransformToPack,
    pkWaitForKeyToExit{$EndIf});
  TParameterKinds = set of TParameterKind;
  TParameterKindStringArray = array[TParameterKind] of Str;

const
  ParameterNames: array[TParameterKind] of Str =
    ('', 'help', 'pack', 'list', 'input', 'output', 'log', 'overwrite', 'press'
    {$IfDef OtherOptions}, 'activate-other-options', 'verify-pack', 'transform-to-sqlite3',
    'transform-to-pack', 'wait-for-key-to-exit'{$EndIf});
  ParameterShortNames: array[TParameterKind] of Char = (#0, #0, #0, 'l', 'i', 'o', #0, 'w', #0
    {$IfDef OtherOptions}, #0, #0, #0, #0, #0{$EndIf});
  ParameterValueRequirements: array[TParameterKind] of Bool =
    (False, False, False, False, True, True, True, False, True
    {$IfDef OtherOptions} , False, False, False, False, False{$EndIf});
  LogParameterValue: TEnumArray<TPackProgramKind, Str> = ('', 'no', 'debug');
  PresssParameterValue: TEnumArray<TPackProgramPress, Str> = ('', 'hard');
  TaskParameters: TParameterKinds = [pkPack, pkList
    {$IfDef OtherOptions} , pkTransformToSQLite3, pkTransformToPack{$EndIf} ];
  {$IfDef OtherOptions}
  OtherParameters: TParameterKinds = [pkVerifyPack, pkTransformToSQLite3, pkTransformToPack, pkWaitForKeyToExit];
  {$EndIf}

type
  TProcessorContext = record
    Task: TProgramTaskKind;
    InputPath, OutputPath: TFileSystemPath;
    Log: TPackProgramKind;
    FileMode: TPackProgramFileHandleMode;
    Press: TPackProgramPress;
    Version: TPackVersion;

    Draft0FileImporter: TPackDraft0FileImporter;
    Draft0FileExporter: TPackDraft0FileExporter;
    Draft0Status: TPackDraft0Status;
  end;
  PProcessorContext = ^TProcessorContext;

var
  Processor: TThread;
  ProcessorContext: TProcessorContext;

function LogError(const AError: Str): Bool;
begin
  Writeln(StdErr, 'Error: ', AError);
  Result := False;
end;

procedure LogHelp;
begin
  WriteLn;
  WriteLn('Pack CLI');
  WriteLn('----');
  WriteLn('Version ', 1);
  WriteLn('https://pack.ac');
  WriteLn('Made by O');
  WriteLn('VAI');
  WriteLn;

  WriteLn('Example:');
  WriteLn('Pack: pack ./test/');
  WriteLn('Unpack: pack ./test.pack');
  WriteLn('Custom: pack [Options]');
  WriteLn;
  WriteLn('Options:');
  WriteLn('--pack');
  //WriteLn('--list, -l'); //Todo
  WriteLn('--input="Path", -i');
  WriteLn('--output="Path", -o');
  WriteLn('--overwrite, -w');
  WriteLn('--press=hard');
  WriteLn('--log=[no, debug]');
  WriteLn;
end;

procedure ProcessMethod(AParameter: Ptr);

  procedure ProcessDraft0(var AContext: TProcessorContext);
  const
    FileModes: array[TPackProgramFileHandleMode] of TPackDraft0FileHandleMode = (
      pd0fhmCreate, pd0fhmCreateOrOverwrite);
    Presses: array[TPackProgramPress] of TPackDraft0Press = (pd0pNone, pd0pHard);
  begin
    with AContext do
      case Task of
        ptkNone: ;
        ptkPack: Start(Draft0FileImporter, &File(OutputPath), FileModes[FileMode], Presses[Press],
            [InputPath], Draft0Status);
        ptkUnpack: Start(Draft0FileExporter, &File(InputPath), FileModes[FileMode], OutputPath, Draft0Status);
        ptkList: WriteLn('Todo!'); //Todo:
        ptkTransformToSQLite3, ptkTransformToPack: Transform(&File(InputPath), &File(OutputPath),
            Task = ptkTransformToSQLite3, FileModes[FileMode], Draft0Status);
      end;
  end;

var
  Context: PProcessorContext absolute AParameter;
begin
  with Context^ do
    case Version of
      pvDraft0: ProcessDraft0(Context^);
      else;
    end;
end;

function Run: Bool;

  function SuggestOutputPath(const APath: TFileSystemPath; ATask: TProgramTaskKind): TFileSystemPath;
  var
    P: TDirectoryPath;
    N: TFileSystemName;
    I: Ind;
  begin
    Split(APath, [sfodpoWithoutPathDelimiter, sfodpoWithoutExtension], P, N);
    for I := 1 to 100 * 1000 do //Retry, and exit anyway to not stuck in a loop
    begin
      Result := P + N;
      if I <> 1 then
        Result += ' (' + ToStr(I) + ')';
      case ATask of
        ptkPack: Result += '.' + PackExtension;
        ptkUnpack: Result += PathDelimiter;
        ptkTransformToSQLite3: Result += '.' + SQLite3Extension;
        ptkTransformToPack: Result += '.' + PackExtension;
        else
          Exit('');
      end;
      if not Exists(FileSystemObject(Result)) then //Check
        Exit;
    end;
    Result := '';
  end;

  function HandleParameters(out ATask: TProgramTaskKind; out AInputPath, AOutputPath: TFileSystemPath;
    out ALog: TPackProgramKind; out AFileMode: TPackProgramFileHandleMode; out APress: TPackProgramPress;
    out AVerify: Bool; out AWaitForKeyToExit: Bool): Bool;

    function Process(const AParameter: TProgramCommandLineParameter;
    var AUsed: TParameterKinds; var AValues: TParameterKindStringArray): Bool; overload;

      function Kind(const AName: Str): TParameterKind;
      begin
        for Result := Low(TParameterKind) to High(TParameterKind) do
          if (AName = ParameterNames[Result]) or (AName = ParameterShortNames[Result]) then
            Exit;
        Result := pkUnknown;
      end;

    var
      N, V: Str;
      K: TParameterKind;
    begin
      N := Name(AParameter);
      V := Value(AParameter);
      K := Kind(N);

      if K in [pkInput, pkOutput] then
        AValues[K] := Fix(V)
      else
        AValues[K] := V;

      //Check repeat
      if K in AUsed then //No repeated parameter is allowed
        Exit(LogError('Repeated parameter: ' + N));
      AUsed += [K];

      //Check requirement
      if K <> pkUnknown then
      begin
        if ParameterValueRequirements[K] and (V = '') then
          Exit(LogError('Missing value for parameter: ' + N));
        if (not ParameterValueRequirements[K]) and (V <> '') then
          Exit(LogError('Value is not allowed for parameter: ' + N));
      end;
      Result := True;
    end;

  var
    PS: TProgramCommandLineParameterArray;
    I: Ind;
    UPS: TParameterKinds = [];
    VS: TParameterKindStringArray;
    {$IfDef OtherOptions}
    K: TParameterKind;
    {$EndIf}
  begin
    Result := False;
    PS := Parameters;
    if PS = nil then
      Exit;
    VS := Default(TParameterKindStringArray);
    for I := 0 to High(PS) do
    begin
      Result := Process(PS[I], UPS, VS);
      if not Result then
        Exit;
      if pkUnknown in UPS then //Check for problem
      begin
        if Length(PS) = 1 then //One and Unknown, maybe Quick mode
        begin
          VS[pkInput] := Fix(VS[pkUnknown]); //Check as file
          if Exists(FileSystemObject(Fix(VS[pkInput]))) then
            UPS := [pkInput]
          else
            Result := False;
        end
        else
          Result := False;
      end;
      if not Result then
        Exit(LogError('Unknown parameter: ' + Condition(Name(PS[I]) <> '', Name(PS[I]), Value(PS[I]))));
    end;

    //Help
    if pkHelp in UPS then
    begin
      LogHelp;
      AWaitForKeyToExit := False;
      Exit(False);
    end;

    //Other Options
    {$IfDef OtherOptions}
    if not (pkActivateOtherOptions in UPS) then //Only accept Other options if present
      for K in OtherParameters do
        if K in UPS then
          Exit(LogError('Unknown parameter: ' + '--' + ParameterNames[K])); //Report as unknown
    {$EndIf}

    //Task
    if TaskParameters * UPS = [pkPack] then
      ATask := ptkPack
    else if TaskParameters * UPS = [pkList] then
      ATask := ptkList
    {$IfDef OtherOptions}
    else if TaskParameters * UPS = [pkTransformToSQLite3] then
      ATask := ptkTransformToSQLite3
    else if TaskParameters * UPS = [pkTransformToPack] then
      ATask := ptkTransformToPack
    {$EndIf}
    else if TaskParameters * UPS = [] then
      ATask := ptkNone //Guess later
    else //More than one task
      Exit(LogError('Unambiguous task'));

    //Input
    if VS[pkInput] = '' then
      Exit(LogError('Missing input'))
    else
      AInputPath := VS[pkInput];

    //Output
    AOutputPath := VS[pkOutput]; //Suggest later if empty

    //Log
    if not (pkLog in UPS) then
      ALog := ppkDefault
    else if not (Find<TPackProgramKind, Str>(LogParameterValue, VS[pkLog], ALog)) then
      Exit(LogError('Unknown value for Log'));

    //FileMode
    AFileMode := Condition(pkOverwrite in UPS, ppfhmCreateOrOverwrite, ppfhmCreate);

    //Press
    if not (pkPress in UPS) then
      APress := pppNone
    else if not (Find<TPackProgramPress, Str>(PresssParameterValue, VS[pkPress], APress)) then
      Exit(LogError('Unknown value for Press'));


    //Others
    {$IfDef OtherOptions}
    AVerify := pkVerifyPack in UPS;
    AWaitForKeyToExit := pkWaitForKeyToExit in UPS;
    {$Else}
    AVerify := False;
    AWaitForKeyToExit := False;
    {$EndIf}

    Result := True;
  end;

  function Check(var ATask: TProgramTaskKind; const AInputPath: TFileSystemPath;
  var AOutputPath: TFileSystemPath; out AVersion: TPackVersion): Bool;
  begin
    Result := False;

    if not Exists(FileSystemObject(AInputPath)) then
      Exit(LogError('Input does not exists'));

    if Kind(AInputPath) = fspkFile then
      AVersion := Version(&File(AInputPath))
    else
      AVersion := pvUnknown;

    //Check Task and Input are a match
    case ATask of
      ptkNone: //Try to guess based on the extension
      begin
        if Extension(AInputPath) = PackExtension then //Export if it has Pack extension
          if AVersion in PackReadVersions then //Only if valid too
          begin
            ATask := ptkUnpack;
            Result := True;
          end
          else
            Result := False
        else
        begin
          ATask := ptkPack; //Import if it is not
          Result := True;
        end;
      end;
      ptkPack:
      begin
        //Todo: Check version on Append
        Result := True;
      end;
      ptkUnpack: Result := True; //Only coming here from Verify
      ptkList: Result := AVersion in PackReadVersions;
      ptkTransformToSQLite3:
      begin
        WriteLn('Warning: Experimental task');
        Result := AVersion in PackReadVersions;
      end;
      ptkTransformToPack:
      begin
        WriteLn('Experimental task. Input will not be verified');
        Result := True; //Try any version if possible, only works if chosen right
      end;
    end;
    if not Result then
      LogError('Not a valid Pack file'); //Looks like a Pack file but it is not supported

    //Suggest Output path
    if (AOutputPath = '') and (ATask in [ptkUnpack, ptkPack, ptkTransformToSQLite3, ptkTransformToPack]) then
    begin
      AOutputPath := SuggestOutputPath(AInputPath, ATask);
      if AOutputPath = '' then
        Exit(LogError('Can not suggest Output path'));
    end;

    case ATask of
      ptkPack: AVersion := PackVersion; //Only Import into current Pack Version
      ptkUnpack:
        if not CreateDirectoryAndParents(Directory(AOutputPath)) then //Make sure it exists
          Exit(LogError('Can not create Output folder'));
      ptkTransformToPack: AVersion := PackVersion; //Only change into current Pack Version
      else;
    end;
  end;

  procedure Log;
  begin
    with ProcessorContext do
    begin
      WriteLn('Task: ', Task);
      WriteLn('Input: ', InputPath);
      WriteLn('Output: ', OutputPath);
      WriteLn('FileMode: ', FileMode);
      if Task = ptkPack then
        WriteLn('Press: ', Press);
      if Task in [ptkPack, ptkTransformToPack] then
        WriteLn('Format Version: ', Version);
    end;
  end;


  function Progress: Bool; overload;
  const
    CheckTime = 1000;
    ProgressSizeLimit = 128 * 1024 * 1024;
  var
    PD, CD: IPS;
    TFL, SP: FPS;
    PT, TI, CT: Tik;

    function ProgressDraft0(out AContinue: Bool): Bool;
    var
      STS: PPackDraft0Statistics = nil;
      ST: TPackDraft0Status;
      DN, TT, P: UPS;
      E: PPackDraft0Error = nil;
      S: Str;
    begin
      //Log Progress
      with ProcessorContext do
        if Log <> ppkNo then
        begin
          case Task of
            ptkPack: STS := Statistics(Draft0FileImporter);
            ptkUnpack: STS := Statistics(Draft0FileExporter);
            else;
          end;

          if STS <> nil then
          begin
            DN := DoneBytes(STS^);
            TT := TotalBytes(STS^);
            if TT <> 0 then
              P := Round(DN / TT * 100)
            else
              P := 0;

            S := ToStr(P) + '%    ' + ToFractionalByteString(DN, busDecimal) + '/' + ToFractionalByteString(TT, busDecimal);

            //Time
            //Todo: Improve
            TI := Tick;
            CT := TI - PT;
            if CT > CheckTime then
            begin
              CD := DN - PD;
              if CD <> 0 then
              begin
                PD := DN;
                PT := TI;
              end;
              if CD <> 0 then
              begin
                if (TFL = 0) or (CD >= ProgressSizeLimit) then
                  TFL := (ProgressSizeLimit / CD) * CT
                else
                  TFL := CT + TFL * (1 - (CD / ProgressSizeLimit));
                SP := ProgressSizeLimit / TFL; //B/ms
              end;
            end;
            S += ' (' + ToFractionalByteString(Round(SP) * 1000, busDecimal) + '/s)';
            System.Write(#13, S, '                                             ');
          end;
        end;

      //Check Status
      ST := ProcessorContext.Draft0Status;
      Result := ST in [pd0sUnknown, pd0sDone]; //Working or Finished successfully
      AContinue := ST = pd0sUnknown;
      if AContinue then
        Exit;

      //Close Progress Log
      if STS <> nil then
        if ProcessorContext.Log <> ppkNo then
          WriteLn;

      //Error
      if not Result then
      begin
        with ProcessorContext do
          case Task of
            ptkPack: E := Error(Draft0FileImporter);
            ptkUnpack: E := Error(Draft0FileExporter);
            else;
          end;
        if E <> nil then
          LogError(AsText(E^, ST))
        else
        begin
          System.Str(ST, S);
          LogError(S); //Improve
        end;
      end;
    end;

  var
    T: Tik;
    C: Bool = True;
  begin
    Result := False;
    T := Tick;

    PD := 0;
    TFL := 0;
    SP := 0;
    PT := Tick;

    with ProcessorContext do
    begin
      while C do
      begin
        Sleep(10);
        case Version of
          pvDraft0: Result := ProgressDraft0(C);
          else;
        end;
      end;
      if Log = ppkDebug then
        WriteLn(Tick - T, 'ms');
    end;
  end;

  function Process: Bool; overload;
  begin
    Processor := Default(TThread);
    Result := Start(Processor, ProcessMethod, @ProcessorContext);
    if not Result then
      Exit(LogError('Can not process'));
    Result := Progress;
    WaitFor(Processor);
    Close(Processor);
  end;

  function Verify: Bool; overload;

    //Todo: Improve
    function Check(const ASourcePaths: TFileSystemPathArray; const AOutputPath: TDirectoryPath): Bool; overload;
    var
      P, CIPA, CIPB: TFileSystemPath;
      CR: TCompareFileResult;
      S: Str;
    begin
      for P in ASourcePaths do
      begin
        CR := UFileCompare.Compare(P, TFileSystemPath(AOutputPath + Name(P, False)), CIPA, CIPB);
        if CR <> cfrMatched then
        begin
          System.Str(CR, S); //Todo: Use helper
          Exit(LogError(S + '. A: ' + CIPA + ', B: ' + CIPB));
        end;
      end;
      Result := True;
    end;

  var
    LG: TPackProgramKind;
    SRC: TFileSystemPath;
  begin
    //Save before change
    LG := ProcessorContext.Log;
    SRC := ProcessorContext.InputPath;

    if LG <> ppkNo then
      System.Write('Verifying...');

    //Unpack
    with ProcessorContext do //Set to Unpack and keep other properties as is
    begin
      Task := ptkUnpack;
      InputPath := OutputPath; //Input is the previous Output
      OutputPath := ''; //Suggest at Check
      Log := ppkNo; //No progress
      FileMode := ppfhmCreate; //Make sure not Overwriting;

      Result := Check(Task, InputPath, OutputPath, Version);
    end;
    if not Result then
      Exit;

    Result := Process;
    if not Result then
      Exit;

    //Check Source and new Output
    Result := Check([SRC], ProcessorContext.OutputPath);

    //Cleanup
    if not Destroy(FileSystemObject(ProcessorContext.OutputPath), nil) then
      Exit(LogError('Problem cleaning up'));

    if not Result then
      Exit;

    if LG <> ppkNo then
      WriteLn(#13, 'Verified.   ');
  end;

var
  VRF, WFKTE: Bool;
begin
  try
    ProcessorContext := Default(TProcessorContext);
    with ProcessorContext do
      Result := HandleParameters(Task, InputPath, OutputPath, Log, FileMode, Press, VRF, WFKTE) and
        Check(Task, InputPath, OutputPath, Version);
    if not Result then
      Exit;

    if ProcessorContext.Log = ppkDebug then
      Log;

    Result := Process;
    if not Result then
      Exit;

    if VRF and (ProcessorContext.Task = ptkPack) then //Only verify Pack task
      Verify;
  finally
    if WFKTE then
    begin
      WriteLn('Press Enter key to exit');
      ReadLn;
    end
    else
    begin
      {$IfNDef Release}
      WriteLn('Done!');
      ReadLn;
      {$EndIf}
    end;
  end;
end;

function Stop: Bool;

  function StopDraft0: Bool;
  var
    ST: TPackDraft0Status;
  begin
    with ProcessorContext do
      case Task of
        ptkPack:
        begin
          Stop(Draft0FileImporter, ST);
          Result := ST = pd0sDone;
        end;
        ptkUnpack:
        begin
          Stop(Draft0FileExporter, ST);
          Result := ST = pd0sDone;
        end;
        else
          Result := False;
      end;
  end;

begin
  with ProcessorContext do
    case Version of
      pvDraft0: Result := StopDraft0;
      else
        Result := False;
    end;

  if Result then
    WaitFor(Processor);
end;

end.
