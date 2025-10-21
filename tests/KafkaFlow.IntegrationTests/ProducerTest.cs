using System;
using System.Threading.Tasks;
using AutoFixture;
using FluentAssertions;
using KafkaFlow.IntegrationTests.Core;
using KafkaFlow.IntegrationTests.Core.Handlers;
using KafkaFlow.IntegrationTests.Core.Producers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace KafkaFlow.IntegrationTests;

[TestClass]
public class ProducerTest
{
    private readonly Fixture _fixture = new();

    private IServiceProvider _provider;

    [TestInitialize]
    public void Setup()
    {
        _provider = Bootstrapper.GetServiceProvider();
        MessageStorage.Clear();
    }

    [TestMethod]
    public async Task ProduceNullKeyTest()
    {
        // Arrange
        var producer = _provider.GetRequiredService<IMessageProducer<GzipProducer>>();
        var message = _fixture.Create<byte[]>();

        // Act
        await producer.ProduceAsync(null, message);

        // Assert
        await MessageStorage.AssertMessageAsync(message);
    }

    [TestMethod]
    public async Task ProduceNullMessageTest()
    {
        // Arrange
        var producer = _provider.GetRequiredService<IMessageProducer<NullProducer>>();
        var key = Guid.NewGuid().ToString();

        // Act
        await producer.ProduceAsync(key, null);

        // Assert
        await MessageStorage.AssertNullMessageAsync();
    }

    [TestMethod]
    public async Task ProduceWithUtcTimestampTest()
    {
        // Arrange
        var producer = _provider.GetRequiredService<IMessageProducer<GzipProducer>>();
        var message = _fixture.Create<byte[]>();
        var key = Guid.NewGuid().ToString();
        var timestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);

        // Act
        var result = await producer.ProduceAsync(key, message, null, null, timestamp);

        // Assert
        result.Should().NotBeNull();
        result.Message.Timestamp.Type.Should().Be(Confluent.Kafka.TimestampType.CreateTime);
        result.Message.Timestamp.UtcDateTime.Should().Be(timestamp);
        await MessageStorage.AssertMessageAsync(message);
    }

    [TestMethod]
    public async Task ProduceWithLocalTimestampTest()
    {
        // Arrange
        var producer = _provider.GetRequiredService<IMessageProducer<GzipProducer>>();
        var message = _fixture.Create<byte[]>();
        var key = Guid.NewGuid().ToString();
        var localTimestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Local);
        var expectedUtcTimestamp = localTimestamp.ToUniversalTime();

        // Act
        var result = await producer.ProduceAsync(key, message, null, null, localTimestamp);

        // Assert
        result.Should().NotBeNull();
        result.Message.Timestamp.Type.Should().Be(Confluent.Kafka.TimestampType.CreateTime);
        result.Message.Timestamp.UtcDateTime.Should().Be(expectedUtcTimestamp);
        await MessageStorage.AssertMessageAsync(message);
    }

    [TestMethod]
    public async Task ProduceWithoutTimestampTest()
    {
        // Arrange
        var producer = _provider.GetRequiredService<IMessageProducer<GzipProducer>>();
        var message = _fixture.Create<byte[]>();
        var key = Guid.NewGuid().ToString();

        // Act
        var result = await producer.ProduceAsync(key, message, null, null, null);

        // Assert
        result.Should().NotBeNull();
        // When no timestamp is provided, broker sets it
        result.Message.Timestamp.Type.Should().NotBe(Confluent.Kafka.TimestampType.NotAvailable);
        await MessageStorage.AssertMessageAsync(message);
    }
}
