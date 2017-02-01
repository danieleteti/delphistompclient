// Stomp Client for Embarcadero Delphi & FreePasca
// Tested With ApacheMQ 5.2/5.3, Apache Apollo 1.2, RabbitMQ
// Copyright (c) 2009-2016 Daniele Teti
//
// Contributors:
// Daniel Gaspary: dgaspary@gmail.com
// Oliver Marr: oliver.sn@wmarr.de
// Marco Mottadelli: mottadelli75@gmail.com
// WebSite: www.danieleteti.it
// email:d.teti@bittime.it
// *******************************************************

unit StompClient;

// For FreePascal users:
// Automatically selected synapse tcp library

{$IFDEF FPC}
{$MODE DELPHI}
{$DEFINE USESYNAPSE}
{$ENDIF}
// For Delphi users:
// Decomment following line to use synapse also in Delphi
{ .$DEFINE USESYNAPSE }

interface

uses
  SysUtils,
  DateUtils,

{$IFNDEF USESYNAPSE}
  IdTCPClient,
  IdException,
  IdExceptionCore,
  IdHeaderList,
  IdIOHandler, IdIOHandlerSocket, IdIOHandlerStack, IdSSL, IdSSLOpenSSL, // SSL
  System.SyncObjs,
{$ELSE}
  synsock,
  blcksock,

{$ENDIF}
  Classes;

const
  LINE_END: char = #10;
  COMMAND_END: char = #0;
  DEFAULT_STOMP_HOST = '127.0.0.1';
  DEFAULT_STOMP_PORT = 61613;

type
  // Add by GC 26/01/2011
  IStompClient = interface;
  IStompFrame = interface;

  // Add by Gc 26/01/2011
  TStompConnectNotifyEvent = procedure (Client: IStompClient; Frame: IStompFrame) of object;

  TAckMode = (amAuto, amClient, amClientIndividual { STOMP 1.1 } );

  TStompAcceptProtocol = (Ver_1_0, Ver_1_1);

  EStomp = class(Exception)
  end;

  TKeyValue = record
    Key: string;
    Value: string;
  end;

  PKeyValue = ^TKeyValue;

  TSenderFrameEvent = procedure(AFrame: IStompFrame) of object;

  StompHeaders = class
    const
      MESSAGE_ID:     string = 'message-id';
      TRANSACTION:    string = 'transaction';
      REPLY_TO:       string = 'reply-to';
      AUTO_DELETE:    string = 'auto-delete';
      CONTENT_LENGTH: string = 'content-length';
      CONTENT_TYPE:   string = 'content-type';
      RECEIPT:        string = 'receipt';
      // RabbitMQ specific headers
      PREFETCH_COUNT: string = 'prefetch-count';
      X_MESSAGE_TTL:  string = 'x-message-ttl';
      X_EXPIRES:      string = 'x-expires';
      TIMESTAMP:      string = 'timestamp';
  end;

  IStompHeaders = interface
    ['{BD087D9D-0576-4C35-88F9-F5D6348E3894}']
    function Add(Key, Value: string): IStompHeaders; overload;
    function Add(HeaderItem: TKeyValue): IStompHeaders; overload;
    function Value(Key: string): string;
    function Remove(Key: string): IStompHeaders;
    function IndexOf(Key: string): Integer;
    function Count: Cardinal;
    function GetAt(const index: Integer): TKeyValue;
    function Output: string;
  end;

  IStompFrame = interface
    ['{68274885-D3C3-4890-A058-03B769B2191E}']
    function Output: string;
    function OutputBytes: TBytes;
    procedure SetHeaders(const Value: IStompHeaders);
    function GetCommand: string;
    procedure SetCommand(const Value: string);
    function GetBody: string;
    procedure SetBody(const Value: string);
    property Body: string read GetBody write SetBody;
    function GetBytesBody: TBytes;
    procedure SetBytesBody(const Value: TBytes);
    property BytesBody: TBytes read GetBytesBody write SetBytesBody;
    function GetHeaders: IStompHeaders;
    function MessageID: string;
    function ContentLength: Integer;
    function ReplyTo: string;
    property Headers: IStompHeaders read GetHeaders write SetHeaders;
    property Command: string read GetCommand write SetCommand;
  end;

  IStompClient = interface
    ['{EDE6EF1D-59EE-4FCC-9CD7-B183E606D949}']
    function Receive(out StompFrame: IStompFrame; ATimeout: Integer)
      : Boolean; overload;
    function Receive: IStompFrame; overload;
    function Receive(ATimeout: Integer): IStompFrame; overload;
    procedure Receipt(const ReceiptID: string);
    function SetHost(Host: string): IStompClient;
    function SetPort(Port: Integer): IStompClient;
    function SetVirtualHost(VirtualHost: string): IStompClient;
    function SetClientId(ClientId: string): IStompClient;
    function SetAcceptVersion(AcceptVersion: TStompAcceptProtocol): IStompClient;
    function Connect: IStompClient;
    function Clone: IStompClient;
    procedure Disconnect;
    procedure Subscribe(QueueOrTopicName: string; Ack: TAckMode = amAuto;
      Headers: IStompHeaders = nil);
    procedure Unsubscribe(Queue: string; const subscriptionId: string = ''); // Unsubscribe STOMP 1.1 : It requires that the id header matches the id value of previous SUBSCRIBE operation.
    procedure Send(QueueOrTopicName: string; TextMessage: string;
      Headers: IStompHeaders = nil); overload;
    procedure Send(QueueOrTopicName: string; TextMessage: string;
      TransactionIdentifier: string; Headers: IStompHeaders = nil); overload;
    procedure Send(QueueOrTopicName: string; ByteMessage: TBytes;
      Headers: IStompHeaders = nil); overload;
    procedure Send(QueueOrTopicName: string; ByteMessage: TBytes;
      TransactionIdentifier: string; Headers: IStompHeaders = nil); overload;

    procedure Ack(const MessageID: string; const subscriptionId: string = '';
      const TransactionIdentifier: string = ''); // ACK  STOMP 1.1 : has two REQUIRED headers: message-id, which MUST contain a value matching the message-id for the MESSAGE being acknowledged and subscription, which MUST be set to match the value of the subscription's id header
    { ** STOMP 1.1 ** }
    procedure Nack(const MessageID: string; const subscriptionId: string = ''; // NACK STOMP 1.1 : takes the same headers as ACK: message-id (mandatory), subscription (mandatory) and transaction (OPTIONAL).
      const TransactionIdentifier: string = '');
    procedure BeginTransaction(const TransactionIdentifier: string);
    procedure CommitTransaction(const TransactionIdentifier: string);
    procedure AbortTransaction(const TransactionIdentifier: string);
    { ****************************************************************** }
    Function SetUseSSL(const boUseSSL: boolean;
      const KeyFile : string =''; const CertFile : string = '';
      const PassPhrase : string = ''): IStompClient; // SSL
    function SetPassword(const Value: string): IStompClient;
    function SetUserName(const Value: string): IStompClient;
    function SetReceiveTimeout(const AMilliSeconds: Cardinal): IStompClient;
    function SetHeartBeat(const OutgoingHeartBeats, IncomingHeartBeats: Int64): IStompClient;
    function Connected: Boolean;
    function GetProtocolVersion: string;
    function GetServer: string;
    function GetSession: string;

    function SetConnectionTimeout(const Value: UInt32): IStompClient;
    function SetOnBeforeSendFrame(const Value: TSenderFrameEvent): IStompClient;
    function SetOnAfterSendFrame(const Value: TSenderFrameEvent): IStompClient;
    function SetOnHeartBeatError(const Value: TNotifyEvent): IStompClient;
    function GetOnConnect: TStompConnectNotifyEvent;
    procedure SetOnConnect(const Value: TStompConnectNotifyEvent);
    property OnConnect: TStompConnectNotifyEvent read GetOnConnect write SetOnConnect;
  end;

  TAddress = record
    Host: string;
    Port: Integer;
    UserName: string;
    Password: string;
  end;

  TAddresses = array of TAddress;

  IStompListener = interface
    ['{CB3EB297-8616-408E-A0B2-7CCC11224DBC}']
    procedure StopListening;
    procedure StartListening;
  end;

  IStompClientListener = interface
    ['{C4C0D932-8994-43FB-9D32-A03FE86AEFE4}']
    procedure OnMessage(StompFrame: IStompFrame; var TerminateListener: Boolean);
    procedure OnListenerStopped(StompClient: IStompClient);
  end;

  StompUtils = class
    class function StompClient: IStompClient;
    class function StompClientAndConnect(Host: string = DEFAULT_STOMP_HOST;
      Port: Integer = DEFAULT_STOMP_PORT;
      VirtualHost: string = '';
      ClientID: string = '';
      AcceptVersion: TStompAcceptProtocol = TStompAcceptProtocol.
      Ver_1_0): IStompClient;
    class function NewDurableSubscriptionHeader(const SubscriptionName: string): TKeyValue;
    class function NewPersistentHeader(const Value: Boolean): TKeyValue;
    class function NewReplyToHeader(const DestinationName: string): TKeyValue;
    class function CreateListener(const StompClient: IStompClient;
      const StompClientListener: IStompClientListener): IStompListener;
    class function StripLastChar(Buf: string; LastChar: char): string;
    class function CreateFrame: IStompFrame;
    class function CreateFrameWithBuffer(Buf: string): IStompFrame;
    class function AckModeToStr(AckMode: TAckMode): string;
    class function NewHeaders: IStompHeaders; deprecated 'Use Headers instead';
    class function Headers: IStompHeaders;
    class function NewFrame: IStompFrame;
    class function TimestampAsDateTime(const HeaderValue: string): TDateTime;
  end;

implementation

{$IFDEF FPC}
const
  CHAR0 = #0;
{$ELSE}
uses
  // Windows,   // Remove windows unit for compiling on ios
  IdGlobal,
  IdGlobalProtocols,
  Character, Winapi.Windows;
{$ENDIF}

type
  TStompFrame = class(TInterfacedObject, IStompFrame)
  private
    FCommand: string;
    FBody: TBytes;
    FContentLength: Integer;
    FHeaders: IStompHeaders;
    procedure SetHeaders(const Value: IStompHeaders);
    function GetCommand: string;
    procedure SetCommand(const Value: string);
    function GetBody: string;
    procedure SetBody(const Value: string);
    function GetBytesBody: TBytes;
    procedure SetBytesBody(const Value: TBytes);
    function GetHeaders: IStompHeaders;

  public
    constructor Create;
    destructor Destroy; override;
    property Command: string read GetCommand write SetCommand;
    property Body: string read GetBody write SetBody;
    property BytesBody: TBytes read GetBytesBody write SetBytesBody;
    // return '', when Key doesn't exist or Value of Key is ''
    // otherwise, return Value;
    function Output: string;
    function OutputBytes: TBytes;
    function MessageID: string;
    function ContentLength: Integer;
    function ReplyTo: string;
    property Headers: IStompHeaders read GetHeaders write SetHeaders;
  end;

  TStompClientListener = class(TInterfacedObject, IStompListener)
  strict private
    FReceiverThread: TThread;
    FTerminated: Boolean;
  private
    FStompClientListener: IStompClientListener;
  strict protected
    FStompClient: IStompClient;
  public
    constructor Create(const StompClient: IStompClient;
      const StompClientListener: IStompClientListener); virtual;
    destructor Destroy; override;
    procedure StartListening;
    procedure StopListening;
  end;

  TReceiverThread = class(TThread)
  private
    FStompClient: IStompClient;
    FStompClientListener: IStompClientListener;
  protected
    procedure Execute; override;
  public
    constructor Create(StompClient: IStompClient; StompClientListener: IStompClientListener);
  end;

  THeartBeatThread = class;

  { TStompClient }
  TStompClient = class(TInterfacedObject, IStompClient)
  private

{$IFDEF USESYNAPSE}
    FSynapseTCP: TTCPBlockSocket;
    FSynapseConnected: boolean;

{$ELSE}
    FTCP: TIdTCPClient;
    FIOHandlerSocketOpenSSL : TIdSSLIOHandlerSocketOpenSSL;
{$ENDIF}
    FOnConnect: TStompConnectNotifyEvent; // Add By GC 26/01/2011
    FHeaders: IStompHeaders;
    FPassword: string;
    FUserName: string;
    FTimeout: Integer;
    FSession: string;
    FInTransaction: boolean;
    FTransactions: TStringList;
    FReceiptTimeout: Integer;
    FServerProtocolVersion: string;
    FClientAcceptProtocolVersion: TStompAcceptProtocol;
    FServer: string;
    FOnBeforeSendFrame: TSenderFrameEvent;
    FOnAfterSendFrame: TSenderFrameEvent;
    FHost: string;
    FPort: Integer;
    FVirtualHost: string;
    FClientID: string;

    FUseSSL   : boolean;      // SSL
    FsslKeyFile : string;     // SSL
    FsslCertFile : string;    // SSL
    FsslKeyPass   : string;   // SSL

    FAcceptVersion: TStompAcceptProtocol;
    FConnectionTimeout: UInt32;
    FOutgoingHeartBeats: Int64;
    FIncomingHeartBeats: Int64;
    FLock: TObject;
    FHeartBeatThread: THeartBeatThread;
    FServerIncomingHeartBeats: Int64;
    FServerOutgoingHeartBeats: Int64;
    FOnHeartBeatError: TNotifyEvent;

    procedure ParseHeartBeat(Headers: IStompHeaders);
    procedure SetReceiptTimeout(const Value: Integer);
    function GetOnConnect: TStompConnectNotifyEvent;
    procedure SetOnConnect(const Value: TStompConnectNotifyEvent);

  protected
{$IFDEF USESYNAPSE}
    procedure SynapseSocketCallBack(Sender: TObject; Reason: THookSocketReason;
      const Value: string);

{$ENDIF}
    procedure Init;
    procedure DeInit;
    procedure MergeHeaders(var AFrame: IStompFrame;
      var AHeaders: IStompHeaders);
    procedure SendFrame(AFrame: IStompFrame; AsBytes: boolean = false);
    procedure SendHeartBeat;
    function FormatErrorFrame(const AErrorFrame: IStompFrame): string;
    function ServerSupportsHeartBeat: boolean;
    procedure OnHeartBeatErrorHandler(Sender: TObject);
    procedure DoHeartBeatErrorHandler;
    procedure OpenSSLGetPassword(var Password: String);
  public
    Function SetUseSSL(const boUseSSL: boolean;
      const KeyFile : string =''; const CertFile : string = '';
      const PassPhrase : string = ''): IStompClient; // SSL

    function SetPassword(const Value: string): IStompClient;
    function SetUserName(const Value: string): IStompClient;
    function Receive(out StompFrame: IStompFrame; ATimeout: Integer)
      : boolean; overload;
    function Receive: IStompFrame; overload;
    function Receive(ATimeout: Integer): IStompFrame; overload;
    procedure Receipt(const ReceiptID: string);
    function SetHost(Host: string): IStompClient;
    function SetPort(Port: Integer): IStompClient;
    function SetVirtualHost(VirtualHost: string): IStompClient;
    function SetClientId(ClientId: string): IStompClient;
    function SetAcceptVersion(AcceptVersion: TStompAcceptProtocol): IStompClient;
    function Connect: IStompClient;
    procedure Disconnect;
    procedure Subscribe(QueueOrTopicName: string;
      Ack: TAckMode = TAckMode.amAuto; Headers: IStompHeaders = nil);
    procedure Unsubscribe(Queue: string; const subscriptionId: string = ''); // Unsubscribe STOMP 1.1 : It requires that the id header matches the id value of previous SUBSCRIBE operation.
    procedure Send(QueueOrTopicName: string; TextMessage: string;
      Headers: IStompHeaders = nil); overload;
    procedure Send(QueueOrTopicName: string; TextMessage: string;
      TransactionIdentifier: string; Headers: IStompHeaders = nil); overload;
    procedure Send(QueueOrTopicName: string; ByteMessage: TBytes;
      Headers: IStompHeaders = nil); overload;
    procedure Send(QueueOrTopicName: string; ByteMessage: TBytes;
      TransactionIdentifier: string; Headers: IStompHeaders = nil); overload;
    procedure Ack(const MessageID: string; const subscriptionId: string = '';
      const TransactionIdentifier: string = ''); // ACK  STOMP 1.1 : has two REQUIRED headers: message-id, which MUST contain a value matching the message-id for the MESSAGE being acknowledged and subscription, which MUST be set to match the value of the subscription's id header
    { STOMP 1.1 }
    procedure Nack(const MessageID: string; const subscriptionId: string = '';
      const TransactionIdentifier: string = ''); // NACK STOMP 1.1 : takes the same headers as ACK: message-id (mandatory), subscription (mandatory) and transaction (OPTIONAL).
    procedure BeginTransaction(const TransactionIdentifier: string);
    procedure CommitTransaction(const TransactionIdentifier: string);
    procedure AbortTransaction(const TransactionIdentifier: string);
    /// ////////////
    constructor Create; overload; virtual;
    destructor Destroy; override;
    function SetHeartBeat(const OutgoingHeartBeats, IncomingHeartBeats: Int64): IStompClient;
    function Clone: IStompClient;
    function Connected: boolean;
    function SetReceiveTimeout(const AMilliSeconds: Cardinal): IStompClient;
    function GetProtocolVersion: string;
    function GetServer: string;
    function GetSession: string;
    property ReceiptTimeout: Integer read FReceiptTimeout
      write SetReceiptTimeout;
    property Transactions: TStringList read FTransactions;
    function SetConnectionTimeout(const Value: UInt32): IStompClient;
    property ConnectionTimeout: UInt32 read FConnectionTimeout
      write FConnectionTimeout;
    // * Manage Events
    function SetOnAfterSendFrame(const Value: TSenderFrameEvent): IStompClient;
    function SetOnBeforeSendFrame(const Value: TSenderFrameEvent): IStompClient;
    function SetOnHeartBeatError(const Value: TNotifyEvent): IStompClient;

    // Add by GC 26/01/2001
    property OnConnect: TStompConnectNotifyEvent read GetOnConnect write SetOnConnect;
  end;

  THeartBeatThread = class(TThread)
  private
    FStompClient: TStompClient;
    FLock: TObject;
    FOutgoingHeatBeatTimeout: Int64;
    FOnHeartBeatError: TNotifyEvent;
  protected
    procedure Execute; override;
    procedure DoHeartBeatError;
  public
    constructor Create(StompClient: TStompClient; Lock: TObject;
      OutgoingHeatBeatTimeout: Int64); virtual;
    property OnHeartBeatError: TNotifyEvent read FOnHeartBeatError write FOnHeartBeatError;
  end;

  TStompHeaders = class(TInterfacedObject, IStompHeaders)
  private
    FList: TList;
    function GetItems(index: Cardinal): TKeyValue;
    procedure SetItems(index: Cardinal; const Value: TKeyValue);

  public
    class function NewDurableSubscriptionHeader(const SubscriptionName: string): TKeyValue;
      deprecated 'Use Subscription instead';
    class function NewPersistentHeader(const Value: Boolean): TKeyValue;
      deprecated 'Use Persistent instead';
    class function NewReplyToHeader(const DestinationName: string): TKeyValue;
      deprecated 'Use ReplyTo instead';

    class function Subscription(const SubscriptionName: string): TKeyValue;
    class function Persistent(const Value: Boolean): TKeyValue;
    class function Durable(const Value: Boolean): TKeyValue;
    class function ReplyTo(const DestinationName: string): TKeyValue;
    /// /
    function Add(Key, Value: string): IStompHeaders; overload;
    function Add(HeaderItem: TKeyValue): IStompHeaders; overload;
    function Value(Key: string): string;
    function Remove(Key: string): IStompHeaders;
    function IndexOf(Key: string): Integer;
    function Count: Cardinal;
    function GetAt(const index: Integer): TKeyValue;
    constructor Create;
    destructor Destroy; override;
    function Output: string;
    property Items[index: Cardinal]: TKeyValue read GetItems
      write SetItems; default;
  end;

class function TStompHeaders.NewDurableSubscriptionHeader(const SubscriptionName
  : string): TKeyValue;
begin
  Result := Subscription(SubscriptionName);
end;

class function TStompHeaders.NewPersistentHeader(const Value: Boolean)
  : TKeyValue;
begin
  Result := Persistent(Value);
end;

class function TStompHeaders.NewReplyToHeader(const DestinationName: string)
  : TKeyValue;
begin
  Result := ReplyTo(DestinationName);
end;

constructor TStompFrame.Create;
begin
  FHeaders := TStompHeaders.Create;
  self.FCommand := '';
  SetLength(self.FBody, 0);
  self.FContentLength := 0;
end;

destructor TStompFrame.Destroy;
begin
  inherited;
end;

function TStompFrame.GetBody: string;
begin
  Result := TEncoding.UTF8.GetString(FBody);
end;

function TStompFrame.GetBytesBody: TBytes;
begin
  Result := FBody;
end;

function TStompFrame.GetCommand: string;
begin
  Result := FCommand;
end;

function TStompFrame.GetHeaders: IStompHeaders;
begin
  Result := FHeaders;
end;

function TStompFrame.MessageID: string;
begin
  Result := self.GetHeaders.Value(StompHeaders.MESSAGE_ID);
end;

function TStompFrame.Output: string;
begin
  Result := FCommand + LINE_END + FHeaders.Output + LINE_END + GetBody +
    COMMAND_END;
end;

function TStompFrame.OutputBytes: TBytes;
begin
  Result := TEncoding.UTF8.GetBytes(FCommand + LINE_END + FHeaders.Output + LINE_END)
    + GetBytesBody + TEncoding.UTF8.GetBytes(COMMAND_END);
end;

function TStompFrame.ReplyTo: string;
begin
  Result := self.GetHeaders.Value(StompHeaders.REPLY_TO);
end;

function TStompFrame.ContentLength: Integer;
begin
  Result := FContentLength;
end;

procedure TStompFrame.SetBody(const Value: string);
begin
  SetBytesBody(TEncoding.UTF8.GetBytes(Value));
end;

procedure TStompFrame.SetBytesBody(const Value: TBytes);
begin
  FBody := Value;
  FContentLength := Length(FBody);
end;

procedure TStompFrame.SetCommand(const Value: string);
begin
  FCommand := Value;
end;

procedure TStompFrame.SetHeaders(const Value: IStompHeaders);
begin
  FHeaders := Value;
end;

function GetLine(Buf: string; var From: Integer): string;
var
  i: Integer;
begin
  if (From > Length(Buf)) then
    raise EStomp.Create('From out of bound.');

  i := From;

  while (i <= Length(Buf)) do
  begin
    if (Buf[i] <> LINE_END) then
      inc(i)
    else
      break;
  end;

  if (Buf[i] = LINE_END) then
  begin
    Result := Copy(Buf, From, i - From);
    From := i + 1;
    exit;
  end
  else
    raise EStomp.Create('End of Line not found.');
end;

{ TStompHeaders }

function TStompHeaders.Add(Key, Value: string): IStompHeaders;
var
  p: PKeyValue;
begin
  New(p);
  p^.Key := Key;
  p^.Value := Value;
  FList.Add(p);
  Result := self;
end;

function TStompHeaders.Add(HeaderItem: TKeyValue): IStompHeaders;
begin
  Result := Add(HeaderItem.Key, HeaderItem.Value);
end;

function TStompHeaders.Count: Cardinal;
begin
  Result := FList.Count;
end;

constructor TStompHeaders.Create;
begin
  inherited;
  FList := TList.Create;
end;

destructor TStompHeaders.Destroy;
var
  i: Integer;
begin
  if FList.Count > 0 then
    for i := 0 to FList.Count - 1 do
      Dispose(PKeyValue(FList[i]));
  FList.Free;
  inherited;
end;

class function TStompHeaders.Durable(const Value: Boolean): TKeyValue;
begin
  Result.Key := 'durable';
  Result.Value := LowerCase(BoolToStr(Value, true));
end;

function TStompHeaders.GetAt(const index: Integer): TKeyValue;
begin
  Result := GetItems(index)
end;

function TStompHeaders.GetItems(index: Cardinal): TKeyValue;
begin
  Result := PKeyValue(FList[index])^;
end;

function TStompHeaders.IndexOf(Key: string): Integer;
var
  i: Integer;
begin
  Result := -1;
  for i := 0 to FList.Count - 1 do
  begin
    if GetItems(i).Key = Key then
    begin
      Result := i;
      break;
    end;
  end;
end;

function TStompHeaders.Output: string;
var
  i: Integer;
  kv: TKeyValue;
begin
  Result := '';
  if FList.Count > 0 then
    for i := 0 to FList.Count - 1 do
    begin
      kv := Items[i];
      Result := Result + kv.Key + ':' + kv.Value + LINE_END;
    end
  else
    Result := LINE_END;
end;

class function TStompHeaders.Persistent(const Value: Boolean): TKeyValue;
begin
  Result.Key := 'persistent';
  Result.Value := LowerCase(BoolToStr(Value, true));
end;

function TStompHeaders.Remove(Key: string): IStompHeaders;
var
  p: Integer;
begin
  p := IndexOf(Key);
  Dispose(PKeyValue(FList[p]));
  FList.Delete(p);
  Result := self;
end;

class function TStompHeaders.ReplyTo(const DestinationName: string): TKeyValue;
begin
  Result.Key := 'reply-to';
  Result.Value := DestinationName;
end;

procedure TStompHeaders.SetItems(index: Cardinal; const Value: TKeyValue);
var
  p: Integer;
begin
  p := IndexOf(Value.Key);
  if p > -1 then
  begin
    PKeyValue(FList[p])^.Key := Value.Key;
    PKeyValue(FList[p])^.Value := Value.Value;
  end
  else
    raise EStomp.Create('Error SetItems');
end;

class function TStompHeaders.Subscription(
  const SubscriptionName: string): TKeyValue;
begin
  Result.Key := 'id';
  Result.Value := SubscriptionName;
end;

function TStompHeaders.Value(Key: string): string;
var
  i: Integer;
begin
  Result := '';
  i := IndexOf(Key);
  if i > -1 then
    Result := GetItems(i).Value;
end;

{ TStompListener }

constructor TStompClientListener.Create(const StompClient: IStompClient;
  const StompClientListener: IStompClientListener);
begin
  FStompClientListener := StompClientListener;
  FStompClient := StompClient;
  FTerminated := False;
  FReceiverThread := nil;
  inherited Create;
end;

destructor TStompClientListener.Destroy;
begin
  FTerminated := true;
  FReceiverThread.Free;
  inherited;
end;

procedure TStompClientListener.StartListening;
begin
  if Assigned(FReceiverThread) then
    raise EStomp.Create('Already listening');
  FReceiverThread := TReceiverThread.Create(FStompClient, FStompClientListener);
  FReceiverThread.Start;
end;

procedure TStompClientListener.StopListening;
begin
  if not Assigned(FReceiverThread) then
    exit;
  FReceiverThread.Terminate;
  FReceiverThread.Free;
  FReceiverThread := nil;
end;

{ TReceiverThread }

constructor TReceiverThread.Create(StompClient: IStompClient;
  StompClientListener: IStompClientListener);
begin
  inherited Create(true);
  FStompClient := StompClient;
  FStompClientListener := StompClientListener;
end;

procedure TReceiverThread.Execute;
var
  LFrame: IStompFrame;
  LTerminateListener: Boolean;
begin
  LTerminateListener := False;
  while (not Terminated) and (not LTerminateListener) do
  begin
    if FStompClient.Receive(LFrame, 1000) then
    begin
      TThread.Synchronize(nil,
        procedure
        begin
          FStompClientListener.OnMessage(LFrame, LTerminateListener);
        end);
    end;
  end;
  TThread.Synchronize(nil,
    procedure
    begin
      FStompClientListener.OnListenerStopped(FStompClient);
    end);
end;

{ TStompClient }

procedure TStompClient.AbortTransaction(const TransactionIdentifier: string);
var
  Frame: IStompFrame;
begin
  if FTransactions.IndexOf(TransactionIdentifier) > -1 then
  begin
    Frame := TStompFrame.Create;
    Frame.Command := 'ABORT';
    Frame.Headers.Add('transaction', TransactionIdentifier);
    SendFrame(Frame);
    FInTransaction := False;
    FTransactions.Delete(FTransactions.IndexOf(TransactionIdentifier));
  end
  else
    raise EStomp.CreateFmt
      ('Abort Transaction Error. Transaction [%s] not found',
      [TransactionIdentifier]);
end;

procedure TStompClient.Ack(const MessageID: string; const subscriptionId: string;
  const TransactionIdentifier: string);
var
  Frame: IStompFrame;
begin
  Frame := TStompFrame.Create;
  Frame.Command := 'ACK';
  Frame.Headers.Add(StompHeaders.MESSAGE_ID, MessageID);

  if subscriptionId <> '' then
    Frame.Headers.Add('subscription', subscriptionId);

  if TransactionIdentifier <> '' then
    Frame.Headers.Add('transaction', TransactionIdentifier);
  SendFrame(Frame);
end;

procedure TStompClient.BeginTransaction(const TransactionIdentifier: string);
var
  Frame: IStompFrame;
begin
  if FTransactions.IndexOf(TransactionIdentifier) = -1 then
  begin
    Frame := TStompFrame.Create;
    Frame.Command := 'BEGIN';
    Frame.Headers.Add('transaction', TransactionIdentifier);
    SendFrame(Frame);
    // CheckReceipt(Frame);
    FInTransaction := True;
    FTransactions.Add(TransactionIdentifier);
  end
  else
    raise EStomp.CreateFmt
      ('Begin Transaction Error. Transaction [%s] still open',
      [TransactionIdentifier]);
end;

// procedure TStompClient.CheckReceipt(Frame: TStompFrame);
// var
// ReceiptID: string;
// begin
// if FEnableReceipts then
// begin
// ReceiptID := inttostr(GetTickCount);
// Frame.Headers.Add('receipt', ReceiptID);
// SendFrame(Frame);
// Receipt(ReceiptID);
// end
// else
// SendFrame(Frame);
// end;

function TStompClient.Clone: IStompClient;
begin
  Result := TStompClient.Create;
  Result
    .SetUserName(FUserName)
    .SetPassword(FPassword)
    .SetConnectionTimeout(FConnectionTimeout)
    .SetHost(FHost)
    .SetPort(FPort)
    .SetVirtualHost(FVirtualHost)
    .SetClientID(FClientId)
    .SetAcceptVersion(FAcceptVersion)
    .Connect;
end;

procedure TStompClient.CommitTransaction(const TransactionIdentifier: string);
var
  Frame: IStompFrame;
begin
  if FTransactions.IndexOf(TransactionIdentifier) > -1 then
  begin
    Frame := TStompFrame.Create;
    Frame.Command := 'COMMIT';
    Frame.Headers.Add('transaction', TransactionIdentifier);
    SendFrame(Frame);
    FInTransaction := False;
    FTransactions.Delete(FTransactions.IndexOf(TransactionIdentifier));
  end
  else
    raise EStomp.CreateFmt
      ('Commit Transaction Error. Transaction [%s] not found',
      [TransactionIdentifier]);
end;

function TStompClient.SetHost(Host: string): IStompClient;
begin
  FHost := Host;
  Result := Self;
end;

function TStompClient.SetPort(Port: Integer): IStompClient;
begin
  FPort := Port;
  Result := Self;
end;

function TStompClient.SetVirtualHost(VirtualHost: string): IStompClient;
begin
  FVirtualHost := VirtualHost;
  Result := Self;
end;

function TStompClient.SetClientId(ClientId: string): IStompClient;
begin
  FClientId := ClientId;
  Result := Self;
end;

function TStompClient.SetAcceptVersion(AcceptVersion: TStompAcceptProtocol): IStompClient;
begin
  FAcceptVersion := AcceptVersion;
  Result := Self;
end;

function TStompClient.Connect: IStompClient;
var
  Frame: IStompFrame;
  lHeartBeat: string;
begin
  try
    Init;

{$IFDEF USESYNAPSE}
    FSynapseConnected := False;
    FSynapseTCP.Connect(FHost, intToStr(FPort));
    FSynapseConnected := True;
{$ELSE}
    if FUseSSL then
    begin
      FIOHandlerSocketOpenSSL.OnGetPassword := OpenSSLGetPassword;
      FIOHandlerSocketOpenSSL.Port := 0  ;
      FIOHandlerSocketOpenSSL.DefaultPort := 0       ;
      FIOHandlerSocketOpenSSL.SSLOptions.Method := sslvTLSv1_2; //sslvSSLv3; //sslvSSLv23;
      FIOHandlerSocketOpenSSL.SSLOptions.KeyFile  := FsslKeyFile;
      FIOHandlerSocketOpenSSL.SSLOptions.CertFile := FsslCertFile;
      FIOHandlerSocketOpenSSL.SSLOptions.Mode := sslmUnassigned; //sslmClient;
      FIOHandlerSocketOpenSSL.SSLOptions.VerifyMode := [];
      FIOHandlerSocketOpenSSL.SSLOptions.VerifyDepth := 0;
//      FIOHandlerSocketOpenSSL.OnBeforeConnect := BeforeConnect;
      FTCP.IOHandler := FIOHandlerSocketOpenSSL;
    end
    else
    begin
      FTCP.IOHandler := nil;
    end;

    FTCP.ConnectTimeout := FConnectionTimeout;
    FTCP.Connect(FHost, FPort);
    FTCP.IOHandler.MaxLineLength := MaxInt;
{$ENDIF}

    Frame := TStompFrame.Create;
    Frame.Command := 'CONNECT';

    FClientAcceptProtocolVersion := FAcceptVersion;
    if TStompAcceptProtocol.Ver_1_1 in [FClientAcceptProtocolVersion]
    then
    begin
      Frame.Headers.Add('accept-version', '1.1'); // stomp 1.1
      lHeartBeat := Format('%d,%d', [FOutgoingHeartBeats, FIncomingHeartBeats]);
      Frame.Headers.Add('heart-beat', lHeartBeat); // stomp 1.1
    end
    else
    begin
      Frame.Headers.Add('accept-version', '1.0'); // stomp 1.0
    end;

    if FVirtualHost <> '' then
    begin
      Frame.Headers.Add('host', FVirtualHost);
    end;

    Frame.Headers.Add('login', FUserName).Add('passcode', FPassword);
    if FClientID <> '' then
    begin
      Frame.Headers.Add('client-id', FClientID);
    end;
    SendFrame(Frame);
    Frame := nil;
    while Frame = nil do
      Frame := Receive;
    if Frame.Command = 'ERROR' then
      raise EStomp.Create(FormatErrorFrame(Frame));
    if Frame.Command = 'CONNECTED' then
    begin
      FSession := Frame.Headers.Value('session');
      FServerProtocolVersion := Frame.Headers.Value('version'); // stomp 1.1
      FServer := Frame.Headers.Value('server'); // stomp 1.1
      ParseHeartBeat(Frame.Headers);
    end;

    // Let's start the hearbeat thread
    if ServerSupportsHeartBeat then
    begin
      FHeartBeatThread := THeartBeatThread.Create(Self, FLock, FServerOutgoingHeartBeats);
      FHeartBeatThread.OnHeartBeatError := OnHeartBeatErrorHandler;
      FHeartBeatThread.Start;
    end;

    { todo: 'Call event?' -> by Gc}
    // Add by GC 26/01/2011
    if Assigned(FOnConnect) then
      FOnConnect(Self, Frame);

  except
    on E: Exception do
    begin
      raise EStomp.Create(E.message);
    end;
  end;

  Result := Self;
end;

function TStompClient.Connected: boolean;
begin

{$IFDEF USESYNAPSE}
  Result := Assigned(FSynapseTCP) and FSynapseConnected;

{$ELSE}                                           // ClosedGracefully <> FTCP.Connected !!!
  Result := Assigned(FTCP) and FTCP.Connected and (not FTCP.IOHandler.ClosedGracefully);

{$ENDIF}
end;

constructor TStompClient.Create;
begin
  inherited;
  FLock := TObject.Create;
  FInTransaction := False;
  FSession := '';
  FUserName := 'guest';
  FPassword := 'guest';
  FUseSSL := false;
  FHeaders := TStompHeaders.Create;
  FTimeout := 200;
  FReceiptTimeout := FTimeout;
  FConnectionTimeout := 1000 * 10; // 10secs
  FIncomingHeartBeats := 10000; // 10secs
  FOutgoingHeartBeats := 0; // disabled

  FHost := '127.0.0.1';
  FPort := DEFAULT_STOMP_PORT;
  FVirtualHost := '';
  FClientID := '';
  FAcceptVersion := TStompAcceptProtocol.Ver_1_0;
end;

procedure TStompClient.DeInit;
begin

{$IFDEF USESYNAPSE}
  FreeAndNil(FSynapseTCP);

{$ELSE}
  FreeAndNil(FTCP);
  FreeAndNil(FIOHandlerSocketOpenSSL);

{$ENDIF}
  FreeAndNil(FTransactions);
end;

destructor TStompClient.Destroy;
begin
  Disconnect;
  DeInit;
  FLock.Free;

  inherited;
end;

procedure TStompClient.Disconnect;
var
  Frame: IStompFrame;
begin
  if Connected then
  begin
    if ServerSupportsHeartBeat then
    begin
      Assert(Assigned(FHeartBeatThread), 'HeartBeat thread not created');
      FHeartBeatThread.Terminate;
      FHeartBeatThread.WaitFor;
      FHeartBeatThread.Free;
    end;

    Frame := TStompFrame.Create;
    Frame.Command := 'DISCONNECT';

    SendFrame(Frame);

{$IFDEF USESYNAPSE}
    FSynapseTCP.CloseSocket;
    FSynapseConnected := False;
{$ELSE}
    FTCP.Disconnect;
{$ENDIF}
  end;
  DeInit;
end;

procedure TStompClient.DoHeartBeatErrorHandler;
begin
  if Assigned(FOnHeartBeatError) then
  begin
    try
      FOnHeartBeatError(Self);
    except
    end;
  end;
end;

function TStompClient.FormatErrorFrame(const AErrorFrame: IStompFrame): string;
begin
  if AErrorFrame.Command <> 'ERROR' then
    raise EStomp.Create('Not an ERROR frame');
  Result := AErrorFrame.Headers.Value('message') + ': ' +
    AErrorFrame.Body;
end;

function TStompClient.GetOnConnect: TStompConnectNotifyEvent;
begin
  Result := FOnConnect;
end;

function TStompClient.GetProtocolVersion: string;
begin
  Result := FServerProtocolVersion;
end;

function TStompClient.GetServer: string;
begin
  Result := FServer;
end;

function TStompClient.GetSession: string;
begin
  Result := FSession;
end;

procedure TStompClient.Init;
begin
  DeInit;

{$IFDEF USESYNAPSE}
  FSynapseTCP := TTCPBlockSocket.Create;
  FSynapseTCP.OnStatus := SynapseSocketCallBack;
  FSynapseTCP.RaiseExcept := True;

{$ELSE}
  FIOHandlerSocketOpenSSL := TIdSSLIOHandlerSocketOpenSSL.Create(nil);
  FTCP := TIdTCPClient.Create(nil);

{$ENDIF}
  FTransactions := TStringList.Create;
end;

{$IFDEF USESYNAPSE}
procedure TStompClient.SynapseSocketCallBack(Sender: TObject;
  Reason: THookSocketReason; const Value: string);
begin
  // As seen at TBlockSocket.ExceptCheck procedure, it SEEMS safe to say
  // when an error occurred and is not a Timeout, the connection is broken
  if (Reason = HR_Error) and (FSynapseTCP.LastError <> WSAETIMEDOUT) then
  begin
    FSynapseConnected := False;
  end;
end;
{$ENDIF}

procedure TStompClient.MergeHeaders(var AFrame: IStompFrame;
  var AHeaders: IStompHeaders);
var
  i: Integer;
  h: TKeyValue;
begin
  if Assigned(AHeaders) then
    if AHeaders.Count > 0 then
      for i := 0 to AHeaders.Count - 1 do
      begin
        h := AHeaders.GetAt(i);
        AFrame.Headers.Add(h.Key, h.Value);
      end;

  // If the frame has some content, then set the length of that content.
  if (AFrame.ContentLength > 0) then
    AFrame.Headers.Add('content-length', intToStr(AFrame.ContentLength));
end;

procedure TStompClient.Nack(const MessageID, subscriptionId, TransactionIdentifier: string);
var
  Frame: IStompFrame;
begin
  Frame := TStompFrame.Create;
  Frame.Command := 'NACK';
  Frame.Headers.Add('message-id', MessageID);

  if subscriptionId <> '' then
    Frame.Headers.Add('subscription', subscriptionId);

  if TransactionIdentifier <> '' then
    Frame.Headers.Add('transaction', TransactionIdentifier);
  SendFrame(Frame);
end;

procedure TStompClient.OnHeartBeatErrorHandler(Sender: TObject);
begin
  FHeartBeatThread.Terminate;
  FHeartBeatThread.WaitFor;
  FHeartBeatThread.Free;
  FHeartBeatThread := nil;
  Disconnect;
  DoHeartBeatErrorHandler;
end;

procedure TStompClient.OpenSSLGetPassword(var Password: String);
begin
  Password := FsslKeyPass;
end;

procedure TStompClient.ParseHeartBeat(Headers: IStompHeaders);
var
  lValue: string;
  lIntValue: string;
begin
  FServerOutgoingHeartBeats := 0;
  FServerIncomingHeartBeats := 0;
  // WARNING!! server heart beat is reversed
  lValue := Headers.Value('heart-beat');
  if Trim(lValue) <> '' then
  begin
    lIntValue := Fetch(lValue, ',');
    FServerIncomingHeartBeats := StrToInt(lIntValue);
    FServerOutgoingHeartBeats := StrToInt(lValue);
  end;
end;

procedure TStompClient.Receipt(const ReceiptID: string);
var
  Frame: IStompFrame;
begin
  if Receive(Frame, FReceiptTimeout) then
  begin
    if Frame.Command <> 'RECEIPT' then
      raise EStomp.Create('Receipt command error');
    if Frame.Headers.Value('receipt-id') <> ReceiptID then
      raise EStomp.Create('Receipt receipt-id error');
  end;
end;

function TStompClient.Receive(out StompFrame: IStompFrame;
  ATimeout: Integer): boolean;
begin
  StompFrame := nil;
  StompFrame := Receive(ATimeout);
  Result := Assigned(StompFrame);
end;

function TStompClient.Receive(ATimeout: Integer): IStompFrame;

{$IFDEF USESYNAPSE}
  function InternalReceiveSynapse(ATimeout: Integer): IStompFrame;
  var
    c: char;
    s: string;
    tout: boolean;
  begin
    tout := False;
    Result := nil;
    try
      try
        FSynapseTCP.SetRecvTimeout(ATimeout);
        s := '';
        try
          while True do
          begin
            c := Chr(FSynapseTCP.RecvByte(ATimeout));
            if c <> CHAR0 then
              s := s + c
              // should be improved with a string buffer (daniele.teti)
            else
            begin
              c := Chr(FSynapseTCP.RecvByte(ATimeout));
              Break;
            end;
          end;
        except
          on E: ESynapseError do
          begin
            if E.ErrorCode = WSAETIMEDOUT then
              tout := True
            else
              raise;
          end;
          on E: Exception do
          begin
            raise;
          end;
        end;
        if not tout then
        begin
          Result := StompUtils.CreateFrame(s + CHAR0);
        end;
      finally
        s := '';
      end;
    except
      on E: Exception do
      begin
        raise;
      end;
    end;
  end;

{$ELSE}
  function InternalReceiveINDY(ATimeout: Integer): IStompFrame;
  var
    lLine: string;
    lSBuilder: TStringBuilder;
    Headers: TIdHeaderList;
    ContentLength: Integer;
    Charset: string;
    lHeartBeat: boolean;
    lTimestampFirstReadLn: TDateTime;
{$IF CompilerVersion < 24}
    Encoding: TIdTextEncoding;
    FreeEncoding: boolean;
{$ELSE}
    Encoding: IIdTextEncoding;
{$ENDIF}
  begin
    Result := nil;
    lSBuilder := TStringBuilder.Create(1024 * 4);
    try
      FTCP.Socket.ReadTimeout := ATimeout;
      FTCP.Socket.DefStringEncoding :=
{$IF CompilerVersion < 24}TIdTextEncoding.UTF8{$ELSE}IndyTextEncoding_UTF8{$ENDIF};

      try
        lTimestampFirstReadLn := Now;
        // read command line
        while True do
        begin
          lLine := FTCP.Socket.ReadLn(LF, ATimeout, -1,
            FTCP.Socket.DefStringEncoding);

          if FTCP.Socket.ReadLnTimedout then
            Break;

          lHeartBeat := lLine = ''; // here is not timeout because of the previous line

          if FServerProtocolVersion = '1.1' then // 1.1 supports heart-beats
          begin
            if (not lHeartBeat) or (lLine <> '') then
              Break;
            if MilliSecondsBetween(lTimestampFirstReadLn, Now) >= ATimeout then
              Break;
          end
          else
            Break; // 1.0
        end;

        if lLine = '' then
          Exit(nil);
        lSBuilder.Append(lLine + LF);

        // read headers
        Headers := TIdHeaderList.Create(QuotePlain);

        try
          repeat
            lLine := FTCP.Socket.ReadLn;
            lSBuilder.Append(lLine + LF);
            if lLine = '' then
              Break;
            // in case of duplicated header, only the first is considered
            // https://stomp.github.io/stomp-specification-1.1.html#Repeated_Header_Entries
            if Headers.IndexOfName(Fetch(lLine, ':', False, False)) = -1 then
              Headers.Add(lLine);
          until False;

          // read body
          //
          // NOTE: non-text data really should be read as a Stream instead of a String!!!
          //
          if IsHeaderMediaType(Headers.Values['content-type'], 'text') then
          begin
            Charset := Headers.Params['content-type', 'charset'];
            if Charset = '' then
              Charset := 'utf-8';
            Encoding := CharsetToEncoding(Charset);
{$IF CompilerVersion < 24}
            FreeEncoding := True;
{$ENDIF}
          end
          else
          begin
            Encoding := IndyTextEncoding_8Bit();
{$IF CompilerVersion < 24}
            FreeEncoding := False;
{$ENDIF}
          end;

{$IF CompilerVersion < 24}
          try
{$ENDIF}
            if Headers.IndexOfName('content-length') <> -1 then
            begin
              // length specified, read exactly that many bytes
              ContentLength := IndyStrToInt(Headers.Values['content-length']);
              if ContentLength > 0 then
              begin
                lLine := FTCP.Socket.ReadString(ContentLength, Encoding);
                lSBuilder.Append(lLine);
              end;
              // frame must still be terminated by a null
              FTCP.Socket.ReadLn(#0 + LF);
            end
            else

            begin
              // no length specified, body terminated by frame terminating null
              lLine := FTCP.Socket.ReadLn(#0 + LF, Encoding);
              lSBuilder.Append(lLine);

            end;
            lSBuilder.Append(#0);
{$IF CompilerVersion < 24}
          finally
            if FreeEncoding then
              Encoding.Free;
          end;
{$ENDIF}
        finally
          Headers.Free;
        end;
      except
        on E: Exception do
        begin
          if lSBuilder.Length > 0 then
            raise EStomp.Create(E.message + sLineBreak + lSBuilder.toString)
          else
            raise;
        end;
      end;
      Result := StompUtils.CreateFrameWithBuffer(lSBuilder.toString);
      if Result.Command = 'ERROR' then
        raise EStomp.Create(FormatErrorFrame(Result));
    finally
      lSBuilder.Free;
    end;
  end;
{$ENDIF}

begin
{$IFDEF USESYNAPSE}
  Result := InternalReceiveSynapse(ATimeout);
{$ELSE}
  Result := InternalReceiveINDY(ATimeout);
{$ENDIF}
end;

function TStompClient.Receive: IStompFrame;
begin
  Result := Receive(FTimeout);
end;

procedure TStompClient.Send(QueueOrTopicName: string; TextMessage: string;
  Headers: IStompHeaders);
var
  Frame: IStompFrame;
begin
  Frame := TStompFrame.Create;
  Frame.Command := 'SEND';
  Frame.Headers.Add('destination', QueueOrTopicName);
  Frame.Body := TextMessage;
  MergeHeaders(Frame, Headers);
  SendFrame(Frame);
end;

procedure TStompClient.Send(QueueOrTopicName: string; TextMessage: string;
  TransactionIdentifier: string; Headers: IStompHeaders);
var
  Frame: IStompFrame;
begin
  Frame := TStompFrame.Create;
  Frame.Command := 'SEND';
  Frame.Headers.Add('destination', QueueOrTopicName);
  Frame.Headers.Add('transaction', TransactionIdentifier);
  Frame.Body := TextMessage;
  MergeHeaders(Frame, Headers);
  SendFrame(Frame);
end;

procedure TStompClient.Send(QueueOrTopicName: string; ByteMessage: TBytes;
  Headers: IStompHeaders);
var
  Frame: IStompFrame;
begin
  Frame := TStompFrame.Create;
  Frame.Command := 'SEND';
  Frame.Headers.Add('destination', QueueOrTopicName);
  Frame.BytesBody := ByteMessage;
  MergeHeaders(Frame, Headers);
  SendFrame(Frame, true);
end;

procedure TStompClient.Send(QueueOrTopicName: string; ByteMessage: TBytes;
  TransactionIdentifier: string; Headers: IStompHeaders);
var
  Frame: IStompFrame;
begin
  Frame := TStompFrame.Create;
  Frame.Command := 'SEND';
  Frame.Headers.Add('destination', QueueOrTopicName);
  Frame.Headers.Add('transaction', TransactionIdentifier);
  Frame.BytesBody := ByteMessage;
  MergeHeaders(Frame, Headers);
  SendFrame(Frame, true);
end;


procedure TStompClient.SendFrame(AFrame: IStompFrame; AsBytes: boolean = false);
begin
  TMonitor.Enter(FLock);
  Try
    if Connected then // Test if error on Socket
    begin
      {$IFDEF USESYNAPSE}
          if Assigned(FOnBeforeSendFrame) then
            FOnBeforeSendFrame(AFrame);
          if AsBytes then
            FSynapseTCP.SendBytes(AFrame.OutputBytes)
          else
            FSynapseTCP.SendString(AFrame.Output);
          if Assigned(FOnAfterSendFrame) then
            FOnAfterSendFrame(AFrame);
      {$ELSE}
          // FTCP.IOHandler.write(TEncoding.ASCII.GetBytes(AFrame.output));
          if Assigned(FOnBeforeSendFrame) then
            FOnBeforeSendFrame(AFrame);

      {$IF CompilerVersion < 25}
          if AsBytes then
            FTCP.IOHandler.Write(AFrame.OutputBytes)
          else
            FTCP.IOHandler.write(TEncoding.UTF8.GetBytes(AFrame.output));
      {$IFEND}
      {$IF CompilerVersion >= 25}
          if AsBytes then
            FTCP.IOHandler.Write(TIdBytes(AFrame.OutputBytes))
          else
            FTCP.IOHandler.write(IndyTextEncoding_UTF8.GetBytes(AFrame.output));
      {$IFEND}

          if Assigned(FOnAfterSendFrame) then
            FOnAfterSendFrame(AFrame);
      {$ENDIF}
    end;
  Finally
    TMonitor.Exit(FLock);
  End;
end;

procedure TStompClient.SendHeartBeat;
begin
  TMonitor.Enter(FLock);
  Try
    if Connected then
    begin
        // Winapi.Windows.Beep(600, 200);
    {$IFDEF USESYNAPSE}
        FSynapseTCP.SendString(LF);
    {$ELSE}

    {$IF CompilerVersion < 25}
        FTCP.IOHandler.write(TEncoding.UTF8.GetBytes(LF));
    {$IFEND}
    {$IF CompilerVersion >= 25}
        FTCP.IOHandler.write(IndyTextEncoding_UTF8.GetBytes(LF));
    {$IFEND}

    {$ENDIF}
    end;
  Finally
    TMonitor.Exit(FLock);
  End;
end;

function TStompClient.ServerSupportsHeartBeat: boolean;
begin
  Result := (FServerProtocolVersion = '1.1') and (FServerOutgoingHeartBeats > 0)
end;

function TStompClient.SetConnectionTimeout(const Value: UInt32): IStompClient;
begin
  FConnectionTimeout := Value;
  Result := Self;
end;

function TStompClient.SetHeartBeat(const OutgoingHeartBeats, IncomingHeartBeats: Int64)
  : IStompClient;
begin
  FOutgoingHeartBeats := OutgoingHeartBeats;
  FIncomingHeartBeats := IncomingHeartBeats;
  Result := Self;
end;

function TStompClient.SetOnAfterSendFrame(const Value: TSenderFrameEvent): IStompClient;
begin
  FOnAfterSendFrame := Value;
  Result := Self;
end;

function TStompClient.SetOnBeforeSendFrame(const Value: TSenderFrameEvent): IStompClient;
begin
  FOnBeforeSendFrame := Value;
  Result := Self;
end;

procedure TStompClient.SetOnConnect(const Value: TStompConnectNotifyEvent);
begin
  FOnConnect := Value;
end;

function TStompClient.SetOnHeartBeatError(const Value: TNotifyEvent): IStompClient;
begin
  FOnHeartBeatError := Value;
  Result := Self;
end;

function TStompClient.SetPassword(const Value: string): IStompClient;
begin
  FPassword := Value;
  Result := Self;
end;

procedure TStompClient.SetReceiptTimeout(const Value: Integer);
begin
  FReceiptTimeout := Value;
end;

function TStompClient.SetReceiveTimeout(const AMilliSeconds: Cardinal)
  : IStompClient;
begin
  FTimeout := AMilliSeconds;
  Result := Self;
end;

function TStompClient.SetUserName(const Value: string): IStompClient;
begin
  FUserName := Value;
  Result := Self;
end;

function TStompClient.SetUseSSL(const boUseSSL: boolean; const KeyFile,
  CertFile, PassPhrase: string): IStompClient;
begin
  FUseSSL := boUseSSL;
  FsslKeyFile   := KeyFile;
  FsslCertFile := CertFile;
  FsslKeyPass  := PassPhrase;
  Result := Self;

end;

procedure TStompClient.Subscribe(QueueOrTopicName: string;
  Ack: TAckMode = TAckMode.amAuto; Headers: IStompHeaders = nil);
var
  Frame: IStompFrame;
begin
  Frame := TStompFrame.Create;
  Frame.Command := 'SUBSCRIBE';
  Frame.Headers.Add('destination', QueueOrTopicName)
    .Add('ack', StompUtils.AckModeToStr(Ack));
  if Headers <> nil then
    MergeHeaders(Frame, Headers);
  SendFrame(Frame);
end;

procedure TStompClient.Unsubscribe(Queue: string; const subscriptionId: string = '');
var
  Frame: IStompFrame;
begin
  Frame := TStompFrame.Create;
  Frame.Command := 'UNSUBSCRIBE';
  Frame.Headers.Add('destination', Queue);

  if subscriptionId <> '' then
    Frame.Headers.Add('id', subscriptionId);

  SendFrame(Frame);
end;

{ THeartBeatThread }

constructor THeartBeatThread.Create(StompClient: TStompClient; Lock: TObject;
  OutgoingHeatBeatTimeout: Int64);
begin
  inherited Create(True);
  FStompClient := StompClient;
  FLock := Lock;
  FOutgoingHeatBeatTimeout := OutgoingHeatBeatTimeout;
end;

procedure THeartBeatThread.DoHeartBeatError;
begin
  if Assigned(FOnHeartBeatError) then
  begin
    try
      // TThread.Synchronize(nil,
      // procedure
      // begin
      // FOnHeartBeatError(Self);
      // end);
    except
      // do nothing here
    end;
  end;
end;

procedure THeartBeatThread.Execute;
var
  lStart: TDateTime;
begin
  while not Terminated do
  begin
    lStart := Now;
    while (not Terminated) and (MilliSecondsBetween(Now, lStart) < FOutgoingHeatBeatTimeout) do
    begin
      Sleep(100);
    end;
    if not Terminated then
    begin
      // If the connection is down, the socket is invalidated so
      // it is not necessary to informa the main thread about
      // such kind of disconnection.
      FStompClient.SendHeartBeat;
    end;
  end;
end;

{ StompUtils }

class function StompUtils.StompClient: IStompClient;
begin
  Result := TStompClient.Create;
end;

class function StompUtils.StompClientAndConnect(Host: string; Port: Integer;
  VirtualHost: string; ClientID: string;
  AcceptVersion: TStompAcceptProtocol): IStompClient;
begin
  Result := Self.StompClient
                .SetHost(Host)
                .SetPort(Port)
                .SetVirtualHost(VirtualHost)
                .SetClientID(ClientID)
                .SetAcceptVersion(AcceptVersion)
                .Connect;
end;

class function StompUtils.NewDurableSubscriptionHeader(const SubscriptionName: string): TKeyValue;
begin
  Result := TStompHeaders.Subscription(SubscriptionName);
end;

class function StompUtils.NewPersistentHeader(const Value: Boolean): TKeyValue;
begin
  Result := TStompHeaders.Persistent(Value);
end;

class function StompUtils.NewReplyToHeader(const DestinationName: string): TKeyValue;
begin
  Result := TStompHeaders.ReplyTo(DestinationName);
end;

class function StompUtils.CreateListener(const StompClient: IStompClient;
  const StompClientListener: IStompClientListener): IStompListener;
begin
  Result := TStompClientListener.Create(StompClient, StompClientListener);
end;

class function StompUtils.StripLastChar(Buf: string; LastChar: char): string;
var
  p: Integer;
begin
  p := Pos(COMMAND_END, Buf);
  if (p = 0) then
    raise EStomp.Create('frame no ending');
  Result := Copy(Buf, 1, p - 1);
end;

class function StompUtils.TimestampAsDateTime(const HeaderValue: string)
  : TDateTime;
begin
  Result := EncodeDateTime(1970, 1, 1, 0, 0, 0, 0) + StrToInt64(HeaderValue)
    / 86400000;
end;

class function StompUtils.AckModeToStr(AckMode: TAckMode): string;
begin
  case AckMode of
    amAuto:
      Result := 'auto';
    amClient:
      Result := 'client';
    amClientIndividual:
      Result := 'client-individual'; // stomp 1.1
  else
    raise EStomp.Create('Unknown AckMode');
  end;
end;

class function StompUtils.NewHeaders: IStompHeaders;
begin
  Result := Headers;
end;

class function StompUtils.CreateFrame: IStompFrame;
begin
  Result := TStompFrame.Create;
end;

class function StompUtils.CreateFrameWithBuffer(Buf: string): IStompFrame;
var
  line: string;
  i: Integer;
  p: Integer;
  Key, Value: string;
  other: string;
  contLen: Integer;
  sContLen: string;
begin
  Result := TStompFrame.Create;
  i := 1;
  try
    Result.Command := GetLine(Buf, i);
    while true do
    begin
      line := GetLine(Buf, i);
      if (line = '') then
        break;
      p := Pos(':', line);
      if (p = 0) then
        raise Exception.Create('header line error');
      Key := Copy(line, 1, p - 1);
      Value := Copy(line, p + 1, Length(line) - p);
      Result.Headers.Add(Key, Value);
    end;
    other := Copy(Buf, i, high(Integer));
    sContLen := Result.Headers.Value('content-length');
    if (sContLen <> '') then
    begin
      if other[Length(other)] <> #0 then
        raise EStomp.Create('frame no ending');
      contLen := StrToInt(sContLen);
      other := StripLastChar(other, COMMAND_END);

      if TEncoding.UTF8.GetByteCount(other) <> contLen then
        // there is still the command_end
        raise EStomp.Create('frame too short');
      Result.Body := other;
    end
    else
    begin
      Result.Body := StripLastChar(other, COMMAND_END)
    end;
  except
    on EStomp do
    begin
      // ignore
      Result := nil;
    end;
    on e: Exception do
    begin
      Result := nil;
      raise EStomp.Create(e.Message);
    end;
  end;
end;

class function StompUtils.Headers: IStompHeaders;
begin
  Result := TStompHeaders.Create;
end;

class function StompUtils.NewFrame: IStompFrame;
begin
  Result := TStompFrame.Create;
end;

end.
