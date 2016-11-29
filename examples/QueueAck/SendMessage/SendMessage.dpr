program SendMessage;

uses
  Vcl.Forms,
  SendMessageForm in 'SendMessageForm.pas' {SendMessageMainForm},
  StompClient in '..\..\..\StompClient.pas';

{$R *.res}


begin
  ReportMemoryLeaksOnShutdown := True;
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TSendMessageMainForm, SendMessageMainForm);
  Application.Run;

end.
