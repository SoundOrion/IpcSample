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