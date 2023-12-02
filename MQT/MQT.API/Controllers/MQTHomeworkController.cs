using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.AspNetCore.Mvc;

namespace MQT.API.Controllers;

[ApiController]
[Route("/api/mqt")]
public class MQTHomeworkController : ControllerBase
{
    private const string _serverUrl = "localhost:9092";
    
    [HttpPost("exercise1/KafkaCLIProducer")]
    public IActionResult KafkaCLIProducer() 
        => Ok("kafka-console-producer.sh --bootstrap-server localhost:9092 --topic kafkaCli");
    
    [HttpGet("exercise2/KafkaCLIConsumer")]
    public IActionResult KafkaCLIConsumer() 
        => Ok("kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafkaCli");

    [HttpPost("exercise3/ProduceStringMessage")]
    public IActionResult ProduceStringMessage(string message)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _serverUrl
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        var produceResult = producer.ProduceAsync("kafkaStringsTopic", new Message<Null, string> { Value = message }).Result;
        return Ok($"Produced message to topic '{produceResult.Topic}', partition {produceResult.Partition}, offset {produceResult.Offset}");
    }

    [HttpGet("exercise4/ConsumeStringMessages")]
    public IActionResult ConsumeStringMessages()
    {
        var config = new ConsumerConfig
        {
            GroupId = $"kafkaStringsTopic-group",
            BootstrapServers = _serverUrl,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        
        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        
        consumer.Subscribe("kafkaStringsTopic");

        var messages = new List<string>();
        try
        {
            while (true)
            {
                try
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(2));
                       
                    Console.WriteLine($"Consumed message '{result.Message.Value}' at: '{result.TopicPartitionOffset}'.");

                    if (result.Message.Value is null)
                        break;
                    
                    messages.Add(result.Message.Value);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occurred: {e.Error.Reason}");
                       
                }
            }
        }
        catch (Exception ex)
        {
            // Ensure the consumer leaves the group cleanly and final offsets are committed.
            consumer.Close();
        }

        return Ok(messages);
    }
    
    [HttpPost("exercise5/ProduceJsonMessage")]
    public IActionResult ProduceJsonMessage(string id, string name, string description)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _serverUrl
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        var message = new { Id = id, Name = name, Description = description };
        var produceResult = producer.ProduceAsync("kafkaJsonTopic", new Message<Null, string> { Value = JsonSerializer.Serialize(message) }).Result;
        return Ok($"Produced message to topic '{produceResult.Topic}', partition {produceResult.Partition}, offset {produceResult.Offset}");
    }

    [HttpGet("exercise6/ConsumeJsonMessages")]
    public IActionResult ConsumeJsonMessages()
    {
        var config = new ConsumerConfig
        {
            GroupId = $"kafkaStringsTopic-group",
            BootstrapServers = _serverUrl,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        
        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        
        consumer.Subscribe("kafkaJsonTopic");

        var messages = new List<dynamic>();
        try
        {
            while (true)
            {
                try
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(2));
                       
                    Console.WriteLine($"Consumed message '{result.Message.Value}' at: '{result.TopicPartitionOffset}'.");

                    if (result.Message.Value is null)
                        break;
                    
                    messages.Add(JsonSerializer.Deserialize<dynamic>(result.Message.Value));
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occurred: {e.Error.Reason}");
                       
                }
            }
        }
        catch (Exception ex)
        {
            // Ensure the consumer leaves the group cleanly and final offsets are committed.
            consumer.Close();
        }

        return Ok(messages);
    }
    
    [HttpPost("exercise7/ProduceEventMessage")]
    public IActionResult ProduceEventMessage(ProductOrdered product)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _serverUrl
        };

        using var producer = new ProducerBuilder<Null, ProductOrdered>(config)
            .SetValueSerializer(new SystemTextJsonKafkaSerializer<ProductOrdered>()).Build();
        
        var produceResult = producer.ProduceAsync("kafkaEventsTopic", new Message<Null, ProductOrdered> { Value = product }).Result;
        return Ok($"Produced message to topic '{produceResult.Topic}', partition {produceResult.Partition}, offset {produceResult.Offset}");
    }

    [HttpGet("exercise8/ConsumeEventsMessages")]
    public IActionResult ConsumeEventsMessages()
    {
        var config = new ConsumerConfig
        {
            GroupId = $"kafkaStringsTopic-group",
            BootstrapServers = _serverUrl,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        
        using var consumer = new ConsumerBuilder<Ignore, ProductOrdered>(config)
            .SetValueDeserializer(new SystemTextJsonKafkaSerializer<ProductOrdered>()).Build();
        
        consumer.Subscribe("kafkaEventsTopic");

        var messages = new List<ProductOrdered>();
        try
        {
            while (true)
            {
                try
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(2));
                       

                    if (result is null || result.Message is null || result.Message.Value is null)
                        break;
                    
                    messages.Add(result.Message.Value);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occurred: {e.Error.Reason}");
                       
                }
            }
        }
        catch (Exception ex)
        {
            // Ensure the consumer leaves the group cleanly and final offsets are committed.
            consumer.Close();
        }

        return Ok(messages);
    }
    
    [HttpPost("exercise9/ProduceEventMessageWithSchema")]
    public IActionResult ProduceEventMessageWithSchema(ProductOrdered product)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _serverUrl
        };
        
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081",
        };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        
        using var producer = new ProducerBuilder<Null, ProductOrdered>(config)
            .SetValueSerializer(new AvroSerializer<ProductOrdered>(schemaRegistry).AsSyncOverAsync())
            .Build();

        
        var produceResult = producer.ProduceAsync("kafkaSchemaTopic", new Message<Null, ProductOrdered> { Value = product }).Result;
        return Ok($"Produced message to topic '{produceResult.Topic}', partition {produceResult.Partition}, offset {produceResult.Offset}");
    }

    [HttpGet("exercise10/ConsumeEventsMessagesWithSchema")]
    public IActionResult ConsumeEventsMessagesWithSchema()
    {
        var config = new ConsumerConfig
        {
            GroupId = $"kafkaStringsTopic-group",
            BootstrapServers = _serverUrl,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081",
        };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        
        using var consumer = new ConsumerBuilder<Ignore, ProductOrdered>(config)
            .SetValueDeserializer(new AvroDeserializer<ProductOrdered>(schemaRegistry).AsSyncOverAsync())
            .Build();
        
        consumer.Subscribe("kafkaSchemaTopic");

        var messages = new List<ProductOrdered>();
        try
        {
            while (true)
            {
                try
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(2));
                    
                    if (result is null || result.Message is null || result.Message.Value is null)
                        break;
                    
                    messages.Add(result.Message.Value);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occurred: {e.Error.Reason}");
                       
                }
            }
        }
        catch (Exception ex)
        {
            // Ensure the consumer leaves the group cleanly and final offsets are committed.
            consumer.Close();
        }

        return Ok(messages);
    }
    
}