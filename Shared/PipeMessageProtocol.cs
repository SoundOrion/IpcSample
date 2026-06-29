using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace Shared;

public static class PipeMessageProtocol
{
    private const int MaxMessageSize = 1024 * 1024; // 1MB
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