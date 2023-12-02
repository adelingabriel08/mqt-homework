using Confluent.Kafka;
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
    
}