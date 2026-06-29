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