program CLIProject;

{$I PackMode.inc}
{$R *.res}

uses
  UProgram,
  UException,
  UConsole,
  UNumber,
  UMain;

  procedure HandleClose;
  begin
    Stop;
  end;

begin
  CloseHandler(HandleClose);
  CursorVisible(False);
  try
    Run;
  finally
    CursorVisible(True);
  end;
end.
