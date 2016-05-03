program Receiver;

uses
  Forms,
  ReceiverForm in 'ReceiverForm.pas' {ReceiverMainForm},
  ThreadReceiver in 'ThreadReceiver.pas';

{$R *.res}

begin
  Application.Initialize;
  //Application.MainFormOnTaskbar := True;
  Application.CreateForm(TReceiverMainForm, ReceiverMainForm);
  Application.Run;
end.
