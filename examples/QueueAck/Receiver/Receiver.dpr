program Receiver;

uses
  Vcl.Forms,
  ReceiverForm in 'ReceiverForm.pas' {ReceiverMainForm},
  ThreadReceiver in 'ThreadReceiver.pas',
  StompClient in '..\..\..\StompClient.pas';

{$R *.res}


begin
  ReportMemoryLeaksOnShutdown := True;
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TReceiverMainForm, ReceiverMainForm);
  Application.Run;

end.
