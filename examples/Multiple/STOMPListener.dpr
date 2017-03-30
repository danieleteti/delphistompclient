program STOMPListener;

uses
  Vcl.Forms,
  MainForm in 'MainForm.pas' {Form4},
  StompClient in '..\..\StompClient.pas';

{$R *.res}


begin
  ReportMemoryLeaksOnShutdown := True;
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TForm4, Form4);
  Application.Run;

end.
