using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using KafkaFlow.Producers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using VerifyMSTest;
using VerifyTests;

namespace KafkaFlow.UnitTests.Producers;

[TestClass]
public class BatchProduceTests : VerifyBase
{
    private readonly VerifySettings _settings;

    public BatchProduceTests()
    {
        _settings = new VerifySettings();
        _settings.DontScrubDateTimes();
    }

    [TestMethod]
    public void BatchProduceItem_WithTimestamp_ShouldStoreTimestamp()
    {
        // Arrange
        var timestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);

        // Act
        var item = new BatchProduceItem("test-topic", "test-key", "test-value", new MessageHeaders(), timestamp);

        // Assert
        item.Timestamp.Should().Be(timestamp);
    }

    [TestMethod]
    public void BatchProduceItem_WithoutTimestamp_ShouldHaveNullTimestamp()
    {
        // Arrange
        var topic = "test-topic";
        var key = "test-key";
        var value = "test-value";
        var headers = new MessageHeaders();

        // Act
        var item = new BatchProduceItem(topic, key, value, headers);

        // Assert
        item.Timestamp.Should().BeNull();
    }

    [TestMethod]
    public async Task BatchProduceAsync_WithTimestamps_ShouldPassTimestampsToProducer()
    {
        // Arrange
        var timestamp1 = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        var timestamp2 = new DateTime(2024, 1, 15, 11, 30, 0, DateTimeKind.Utc);

        var items = new List<BatchProduceItem>
        {
            new ("topic1", "key1", "value1", new MessageHeaders(), timestamp1),
            new ("topic2", "key2", "value2", new MessageHeaders(), timestamp2),
            new ("topic3", "key3", "value3", new MessageHeaders(), timestamp: null)
        };

        var producerMock = new MockProducer();

        // Act
        await producerMock.Object.BatchProduceAsync(items, throwIfAnyProduceFail: false);

        // Assert
        var capturedTimestamps = producerMock.CapturedCalls.Select(c => c.Timestamp).ToList();


        await Verify(capturedTimestamps, _settings);
    }

    [TestMethod]
    public async Task BatchProduceAsync_MixedTimestamps_ShouldHandleCorrectly()
    {
        // Arrange
        var utcTimestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);

        var items = new List<BatchProduceItem>
        {
            new ("topic1", "key1", "value1", new MessageHeaders(), utcTimestamp),
            new ("topic2", "key2", "value2", new MessageHeaders()),
        };

        var producerMock = new MockProducer();

        // Act
        var result = await producerMock.Object.BatchProduceAsync(items, throwIfAnyProduceFail: false);

        // Assert
        producerMock.CapturedCalls.Should().HaveCount(2);
        result.Should().HaveCount(2);
        result.All(x => x.DeliveryReport != null).Should().BeTrue();
    }

    [TestMethod]
    public async Task BatchProduceAsync_AllItemsWithSameTimestamp_ShouldProduceSuccessfully()
    {
        // Arrange
        var sharedTimestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);

        var items = new List<BatchProduceItem>
        {
            new ("topic1", "key1", "value1", new MessageHeaders(), sharedTimestamp),
            new ("topic2", "key2", "value2", new MessageHeaders(), sharedTimestamp),
            new ("topic3", "key3", "value3", new MessageHeaders(), sharedTimestamp)
        };

        var producerMock = new MockProducer();

        // Act
        var result = await producerMock.Object.BatchProduceAsync(items, throwIfAnyProduceFail: false);

        // Assert
        result.Should().HaveCount(3);
        result.All(x => x.DeliveryReport != null).Should().BeTrue();
        producerMock.CapturedCalls.Should().HaveCount(3);
        producerMock.CapturedCalls.Should().OnlyContain(c => c.Timestamp == sharedTimestamp);
        producerMock.Verify(
            x => x.Produce(
                It.IsAny<string>(),
                It.IsAny<object>(),
                It.IsAny<object>(),
                It.IsAny<IMessageHeaders>(),
                It.IsAny<Action<DeliveryReport<byte[], byte[]>>>(),
                null,
                sharedTimestamp),
            Times.Exactly(3));
    }
}
