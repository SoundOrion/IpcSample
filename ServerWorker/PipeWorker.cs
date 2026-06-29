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

                    await PipeMessageProtocol.WriteJsonAsync(pipe, errorResponse, CancellationToken.None);
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