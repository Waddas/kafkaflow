using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Moq;

namespace KafkaFlow.UnitTests.Producers;

/// <summary>
/// Reusable mock producer for testing produce functionality.
/// </summary>
public class MockProducer : Mock<IMessageProducer>
{
    private readonly List<CapturedProduceCall> _capturedCalls = [];

    public MockProducer()
    {
        SetupProduceMethod();
        SetupProduceAsyncMethod();
    }

    public IReadOnlyList<CapturedProduceCall> CapturedCalls => _capturedCalls;

    private void SetupProduceMethod(bool shouldSucceed = true)
    {
        Setup(x => x.Produce(
                It.IsAny<string>(),
                It.IsAny<object>(),
                It.IsAny<object>(),
                It.IsAny<IMessageHeaders>(),
                It.IsAny<Action<DeliveryReport<byte[], byte[]>>>(),
                It.IsAny<int?>(),
                It.IsAny<DateTime?>()))
            .Callback<string, object, object, IMessageHeaders, Action<DeliveryReport<byte[], byte[]>>, int?, DateTime?>(
                (topic, key, value, headers, handler, partition, timestamp) =>
                {
                    _capturedCalls.Add(new CapturedProduceCall
                    {
                        Topic = topic,
                        Key = key,
                        Value = value,
                        Headers = headers,
                        Partition = partition,
                        Timestamp = timestamp
                    });

                    handler?.Invoke(new DeliveryReport<byte[], byte[]>
                    {
                        Topic = topic,
                        Error = shouldSucceed ? new Error(ErrorCode.NoError) : new Error(ErrorCode.Local_AllBrokersDown)
                    });
                });
    }

    private void SetupProduceAsyncMethod(bool shouldSucceed = true)
    {
        Setup(x => x.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<object>(),
                It.IsAny<object>(),
                It.IsAny<IMessageHeaders>(),
                It.IsAny<int?>(),
                It.IsAny<DateTime?>()))
            .Callback<string, object, object, IMessageHeaders, int?, DateTime?>(
                (topic, key, value, headers, partition, timestamp) =>
                {
                    _capturedCalls.Add(new CapturedProduceCall
                    {
                        Topic = topic,
                        Key = key,
                        Value = value,
                        Headers = headers,
                        Partition = partition,
                        Timestamp = timestamp
                    });
                })
            .ReturnsAsync(new DeliveryResult<byte[], byte[]>
            {
                Status = shouldSucceed ? PersistenceStatus.Persisted : PersistenceStatus.NotPersisted
            });
    }

    public void ClearCapturedCalls()
    {
        _capturedCalls.Clear();
    }
}

public class CapturedProduceCall
{
    public string Topic { get; init; }
    public object Key { get; init; }
    public object Value { get; init; }
    public IMessageHeaders Headers { get; init; }
    public int? Partition { get; init; }
    public DateTime? Timestamp { get; init; }
}
