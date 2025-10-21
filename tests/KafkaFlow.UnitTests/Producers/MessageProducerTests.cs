using System;
using System.Threading.Tasks;
using FluentAssertions;
using KafkaFlow.Producers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace KafkaFlow.UnitTests.Producers;

[TestClass]
public class MessageProducerTests
{
    [TestMethod]
    public async Task ProduceAsync_WithTimestamp_ShouldPassTimestampToUnderlyingProducer()
    {
        // Arrange
        var timestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        var innerProducerMock = new MockProducer();

        var wrapper = new MessageProducerWrapper<object>(innerProducerMock.Object);

        // Act
        await wrapper.ProduceAsync("test-topic", "key", "value", null, null, timestamp);

        // Assert
        innerProducerMock.CapturedCalls.Should().ContainSingle();
        innerProducerMock.CapturedCalls[0].Timestamp.Should().Be(timestamp);
        innerProducerMock.Verify(x => x.ProduceAsync("test-topic", "key", "value", null, null, timestamp), Times.Once);
    }

    [TestMethod]
    public async Task ProduceAsync_WithoutTimestamp_ShouldPassNullToUnderlyingProducer()
    {
        // Arrange
        var innerProducerMock = new MockProducer();

        var wrapper = new MessageProducerWrapper<object>(innerProducerMock.Object);

        // Act
        await wrapper.ProduceAsync("test-topic", "key", "value", null, null, null);

        // Assert
        innerProducerMock.CapturedCalls.Should().ContainSingle();
        innerProducerMock.CapturedCalls[0].Timestamp.Should().BeNull();
    }

    [TestMethod]
    public void Produce_WithTimestamp_ShouldPassTimestampToUnderlyingProducer()
    {
        // Arrange
        var timestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        var innerProducerMock = new MockProducer();

        var wrapper = new MessageProducerWrapper<object>(innerProducerMock.Object);

        // Act
        wrapper.Produce("test-topic", "key", "value", null, null, null, timestamp);

        // Assert
        innerProducerMock.CapturedCalls.Should().ContainSingle();
        innerProducerMock.CapturedCalls[0].Timestamp.Should().Be(timestamp);
        innerProducerMock.Verify(x => x.Produce("test-topic", "key", "value", null, null, null, timestamp), Times.Once);
    }

    [TestMethod]
    public void Produce_WithoutTimestamp_ShouldPassNullToUnderlyingProducer()
    {
        // Arrange
        var innerProducerMock = new MockProducer();

        var wrapper = new MessageProducerWrapper<object>(innerProducerMock.Object);

        // Act
        wrapper.Produce("test-topic", "key", "value", null, null, null, null);

        // Assert
        innerProducerMock.CapturedCalls.Should().ContainSingle();
        innerProducerMock.CapturedCalls[0].Timestamp.Should().BeNull();
    }

    [TestMethod]
    public void Timestamp_ConversionToUtc_ShouldWorkCorrectly()
    {
        // This tests the DateTime.ToUniversalTime() behavior we rely on

        // UTC timestamp should remain unchanged
        var utcTimestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        utcTimestamp.ToUniversalTime().Should().Be(utcTimestamp);

        // Local timestamp should convert to UTC
        var localTimestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Local);
        localTimestamp.ToUniversalTime().Kind.Should().Be(DateTimeKind.Utc);

        // Unspecified should convert (treating as local)
        var unspecifiedTimestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Unspecified);
        unspecifiedTimestamp.ToUniversalTime().Kind.Should().Be(DateTimeKind.Utc);
    }
}
