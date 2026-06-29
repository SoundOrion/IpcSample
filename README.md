# .NET 8 IPC Sample

C# / .NET 8 で、常駐サーバーである Worker Service と、クライアントアプリケーションの間でプロセス間通信を行うサンプルです。

クライアントがメッセージを送信し、サーバーが処理した結果をクライアントに返します。

IPC には **Named Pipes** を使用します。

---

## 概要

このサンプルでは、以下の構成で IPC を実装します。

```txt
ClientApp
   |
   | Named Pipe
   v
ServerWorker
```

* `ServerWorker`

  * 常駐する Worker Service
  * Named Pipe サーバーとして待ち受ける
  * クライアントからのリクエストを処理してレスポンスを返す

* `ClientApp`

  * コンソールアプリケーション
  * Named Pipe クライアントとしてサーバーに接続する
  * コマンドとペイロードを送信する

* `Shared`

  * クライアントとサーバーで共有するメッセージ定義
  * JSON メッセージの読み書き用プロトコル

---

## 使用技術

* .NET 8
* C#
* Worker Service
* Named Pipes
* JSON
* Length-prefix protocol

---

## プロジェクト構成

```txt
IpcSample/
  Shared/
    IpcMessage.cs
    PipeMessageProtocol.cs

  ServerWorker/
    Program.cs
    PipeWorker.cs

  ClientApp/
    Program.cs
```

---

## プロジェクト作成

```bash
dotnet new sln -n IpcSample

dotnet new classlib -n Shared -f net8.0
dotnet new worker -n ServerWorker -f net8.0
dotnet new console -n ClientApp -f net8.0

dotnet sln add Shared/Shared.csproj
dotnet sln add ServerWorker/ServerWorker.csproj
dotnet sln add ClientApp/ClientApp.csproj

dotnet add ServerWorker/ServerWorker.csproj reference Shared/Shared.csproj
dotnet add ClientApp/ClientApp.csproj reference Shared/Shared.csproj
```

---

## Shared/IpcMessage.cs

```csharp
namespace Shared;

public sealed record IpcRequest(
    string Command,
    string? Payload
);

public sealed record IpcResponse(
    bool Success,
    string? Result,
    string? Error
);
```

---

## Shared/PipeMessageProtocol.cs

Named Pipe はストリームなので、そのまま JSON を流すだけでは、どこまでが 1 メッセージなのか判別しづらくなります。

そのため、このサンプルでは以下の形式でメッセージを送受信します。

```txt
[4バイトのメッセージ長][JSON本文]
```

```csharp
using System.Buffers.Binary;
using System.Text.Json;

namespace Shared;

public static class PipeMessageProtocol
{
    private const int MaxMessageSize = 1024 * 1024;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static async Task WriteJsonAsync<T>(
        Stream stream,
        T value,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(value, JsonOptions);

        if (payload.Length > MaxMessageSize)
        {
            throw new InvalidOperationException("Message is too large.");
        }

        byte[] lengthPrefix = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(lengthPrefix, payload.Length);

        await stream.WriteAsync(lengthPrefix, cancellationToken);
        await stream.WriteAsync(payload, cancellationToken);
        await stream.FlushAsync(cancellationToken);
    }

    public static async Task<T?> ReadJsonAsync<T>(
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        byte[] lengthPrefix = new byte[4];

        try
        {
            await stream.ReadExactlyAsync(lengthPrefix, cancellationToken);
        }
        catch (EndOfStreamException)
        {
            return default;
        }

        int length = BinaryPrimitives.ReadInt32LittleEndian(lengthPrefix);

        if (length <= 0 || length > MaxMessageSize)
        {
            throw new InvalidOperationException($"Invalid message size: {length}");
        }

        byte[] payload = new byte[length];
        await stream.ReadExactlyAsync(payload, cancellationToken);

        return JsonSerializer.Deserialize<T>(payload, JsonOptions);
    }
}
```

---

## ServerWorker/Program.cs

```csharp
using ServerWorker;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<PipeWorker>();

IHost host = builder.Build();
host.Run();
```

---

## ServerWorker/PipeWorker.cs

```csharp
using System.IO.Pipes;
using Shared;

namespace ServerWorker;

public sealed class PipeWorker : BackgroundService
{
    private const string PipeName = "my-app-ipc-pipe";
    private readonly ILogger<PipeWorker> _logger;

    public PipeWorker(ILogger<PipeWorker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("IPC server started. PipeName={PipeName}", PipeName);

        while (!stoppingToken.IsCancellationRequested)
        {
            var pipe = new NamedPipeServerStream(
                PipeName,
                PipeDirection.InOut,
                NamedPipeServerStream.MaxAllowedServerInstances,
                PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous | PipeOptions.CurrentUserOnly);

            try
            {
                await pipe.WaitForConnectionAsync(stoppingToken);

                _ = Task.Run(
                    () => HandleClientAsync(pipe, stoppingToken),
                    CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                await pipe.DisposeAsync();
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed while waiting for pipe connection.");
                await pipe.DisposeAsync();
            }
        }

        _logger.LogInformation("IPC server stopped.");
    }

    private async Task HandleClientAsync(
        NamedPipeServerStream pipe,
        CancellationToken cancellationToken)
    {
        await using (pipe)
        {
            try
            {
                IpcRequest? request =
                    await PipeMessageProtocol.ReadJsonAsync<IpcRequest>(pipe, cancellationToken);

                if (request is null)
                {
                    return;
                }

                _logger.LogInformation(
                    "Received request. Command={Command}, Payload={Payload}",
                    request.Command,
                    request.Payload);

                IpcResponse response = await ProcessRequestAsync(request, cancellationToken);

                await PipeMessageProtocol.WriteJsonAsync(pipe, response, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Client handling failed.");

                if (pipe.IsConnected)
                {
                    var errorResponse = new IpcResponse(
                        Success: false,
                        Result: null,
                        Error: ex.Message);

                    await PipeMessageProtocol.WriteJsonAsync(
                        pipe,
                        errorResponse,
                        CancellationToken.None);
                }
            }
        }
    }

    private static Task<IpcResponse> ProcessRequestAsync(
        IpcRequest request,
        CancellationToken cancellationToken)
    {
        IpcResponse response = request.Command switch
        {
            "echo" => new IpcResponse(
                Success: true,
                Result: request.Payload,
                Error: null),

            "upper" => new IpcResponse(
                Success: true,
                Result: request.Payload?.ToUpperInvariant(),
                Error: null),

            "time" => new IpcResponse(
                Success: true,
                Result: DateTimeOffset.Now.ToString("O"),
                Error: null),

            _ => new IpcResponse(
                Success: false,
                Result: null,
                Error: $"Unknown command: {request.Command}")
        };

        return Task.FromResult(response);
    }
}
```

---

## ClientApp/Program.cs

```csharp
using System.IO.Pipes;
using Shared;

const string PipeName = "my-app-ipc-pipe";

string command = args.Length >= 1 ? args[0] : "echo";
string? payload = args.Length >= 2 ? args[1] : "hello from client";

await using var pipe = new NamedPipeClientStream(
    ".",
    PipeName,
    PipeDirection.InOut,
    PipeOptions.Asynchronous);

Console.WriteLine("Connecting to IPC server...");

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
await pipe.ConnectAsync(cts.Token);

var request = new IpcRequest(
    Command: command,
    Payload: payload);

await PipeMessageProtocol.WriteJsonAsync(pipe, request, cts.Token);

IpcResponse? response =
    await PipeMessageProtocol.ReadJsonAsync<IpcResponse>(pipe, cts.Token);

if (response is null)
{
    Console.WriteLine("No response.");
    return;
}

if (response.Success)
{
    Console.WriteLine($"OK: {response.Result}");
}
else
{
    Console.WriteLine($"ERROR: {response.Error}");
}
```

---

## 実行方法

サーバーを起動します。

```bash
dotnet run --project ServerWorker
```

別のターミナルでクライアントを実行します。

```bash
dotnet run --project ClientApp -- echo "こんにちは"
```

実行結果例:

```txt
Connecting to IPC server...
OK: こんにちは
```

---

## コマンド例

### echo

送信した文字列をそのまま返します。

```bash
dotnet run --project ClientApp -- echo "hello"
```

```txt
OK: hello
```

---

### upper

送信した文字列を大文字に変換して返します。

```bash
dotnet run --project ClientApp -- upper "hello ipc"
```

```txt
OK: HELLO IPC
```

---

### time

サーバー側の現在時刻を返します。

```bash
dotnet run --project ClientApp -- time
```

```txt
OK: 2026-06-29T12:34:56.7890123+09:00
```

---

## 複数クライアント対応について

サーバー側では、クライアント接続を受け付けた後、処理を別タスクに渡しています。

```csharp
_ = Task.Run(
    () => HandleClientAsync(pipe, stoppingToken),
    CancellationToken.None);
```

これにより、1つのクライアントを処理している間も、サーバーは次のクライアント接続を受け付けることができます。

---

## メッセージ形式

このサンプルでは、以下の JSON を送受信します。

### リクエスト

```json
{
  "command": "upper",
  "payload": "hello"
}
```

### レスポンス

```json
{
  "success": true,
  "result": "HELLO",
  "error": null
}
```

失敗時の例:

```json
{
  "success": false,
  "result": null,
  "error": "Unknown command: unknown"
}
```

---

## Windows サービスとして動かす場合

開発中は `dotnet run` で十分ですが、本番では `ServerWorker` を Windows サービスとして常駐させることもできます。

パッケージを追加します。

```bash
dotnet add ServerWorker package Microsoft.Extensions.Hosting.WindowsServices
```

`ServerWorker/Program.cs` を以下のように変更します。

```csharp
using ServerWorker;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.Services.AddWindowsService(options =>
{
    options.ServiceName = "My App IPC Worker";
});

builder.Services.AddHostedService<PipeWorker>();

IHost host = builder.Build();
host.Run();
```

publish します。

```bash
dotnet publish ServerWorker -c Release -r win-x64 --self-contained false
```

サービスとして登録します。

```powershell
sc.exe create "My App IPC Worker" binpath= "C:\path\to\ServerWorker.exe"
sc.exe start "My App IPC Worker"
```

停止する場合:

```powershell
sc.exe stop "My App IPC Worker"
```

削除する場合:

```powershell
sc.exe delete "My App IPC Worker"
```

---

## 実運用で追加するとよいもの

このサンプルは最小構成です。

実運用では、必要に応じて以下を追加します。

| 項目         | 目的                       |
| ---------- | ------------------------ |
| リクエストID    | ログ追跡をしやすくする              |
| タイムアウト     | ハングを防ぐ                   |
| キャンセル      | 長時間処理を中断できるようにする         |
| 認可         | 想定外のクライアントからのアクセスを防ぐ     |
| プロトコルバージョン | クライアントとサーバーのバージョン差分に対応する |
| キュー処理      | 重い処理の並列数を制御する            |
| ログ出力       | 障害調査をしやすくする              |
| リトライ       | 一時的な接続失敗に対応する            |

---

## IPC 方式の選択

今回のように、同一マシン内で常駐ワーカーとクライアントが通信する場合は、Named Pipes が扱いやすいです。

| 方式               | 向いているケース                   |
| ---------------- | -------------------------- |
| Named Pipes      | 同一マシン内の IPC                |
| TCP localhost    | 将来的に別マシン接続も考える場合           |
| gRPC             | 型付き API、ストリーミング、拡張性を重視する場合 |
| MemoryMappedFile | 大容量データを高速に共有したい場合          |
| Queue            | 即時レスポンスが不要な非同期ジョブ処理        |

---

## 注意点

### JSON をそのまま流さない

Named Pipe はストリームなので、JSON をそのまま書き込むだけでは、受信側がメッセージの区切りを正しく判断できない場合があります。

このサンプルでは、先頭に 4 バイトのメッセージ長を付けることで、1メッセージの範囲を明確にしています。

---

### サーバー側の例外処理

クライアント処理中に例外が発生した場合、可能であればエラーレスポンスを返します。

```json
{
  "success": false,
  "result": null,
  "error": "error message"
}
```

---

### セキュリティ

このサンプルでは、サーバー側の Named Pipe 作成時に以下を指定しています。

```csharp
PipeOptions.CurrentUserOnly
```

これにより、同じユーザーのプロセスからのみ接続できるようにしています。

より細かいアクセス制御が必要な場合は、OS や実行環境に応じた追加の制御を検討してください。

---

## ライセンス

このサンプルコードは自由に利用・改変できます。
