program consumer;

{$APPTYPE CONSOLE}

{$R *.res}

{
  https://www.rabbitmq.com/tutorials/tutorial-one-python.html
}

uses
  System.SysUtils, 
  StompClient in '..\..\StompClient.pas';

procedure Main;
var
  lClient: IStompClient;
  lStompFrame: IStompFrame;
begin
  lClient := StompUtils.StompClient;
  lClient.Connect();
  WriteLn('Subscribing to queue "myqueue"');
  lClient.Subscribe('/queue/myqueue');

  WriteLn('Reading just the following 2 messages...');
  WriteLn(sLineBreak + sLineBreak + 'Waiting for the 1st message...' + sLineBreak +
    StringOfChar('*', 40));

  if lClient.Receive(lStompFrame, 5000) then
  begin
    WriteLn('MESSAGE: ' + lStompFrame.GetBody);
  end
  else
  begin
    WriteLn('Cannot read message after timeout...');
  end;

  WriteLn(sLineBreak + sLineBreak + 'Waiting for the 2nd message...' + sLineBreak +
    StringOfChar('*', 40));
  if lClient.Receive(lStompFrame, 5000) then
  begin
    WriteLn('MESSAGE: ' + lStompFrame.GetBody);
  end
  else
    WriteLn('Cannot read message after timeout...');

  WriteLn('Closing');
  lClient.Disconnect;
end;

begin
  try
    Main;
    Write('Press return to quit');
    ReadLn;
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName, ': ', E.Message);
      ReadLn;
    end;
  end;

end.
