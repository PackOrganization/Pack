program CLIProject;

{$I PackMode.inc}
{$R *.res}

uses
  UProgram,
  UException,
  UConsole,
  UNumber,
  UMain;

var
  Stopped: Bool = False;

  procedure HandleClose;
  begin
    Stopped := True;
    CursorVisible(True);
    Stop;
  end;

begin
  CloseHandler(HandleClose);
  CursorVisible(False);
  try
    Run;
  finally
    if not Stopped then //Improve: Prevent freeze on Linux
      CursorVisible(True);
  end;
end.
