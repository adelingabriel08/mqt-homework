using System.Text.Json;
using Confluent.Kafka;

namespace MQT.API;

public class SystemTextJsonKafkaSerializer<T> : ISerializer<T>, IDeserializer<T>
{
    private JsonSerializerOptions DefaultOptions { get; set; }

    public byte[] Serialize(T data, SerializationContext context)
        => JsonSerializer.SerializeToUtf8Bytes(data, DefaultOptions);

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        => JsonSerializer.Deserialize<T>(data, DefaultOptions)!;
}