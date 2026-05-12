// SENDER

// Manulal message sending in RabbitMQ
//  1. Navigate to exchanges
//  2. Select a exchange
//  3. Provide routing key
//  4. Provide message body (JSON)

using RabbitMQ.Client;
using System.Text;
using System.Text.Json;
using RabbitShared;
using System;

// Object responsible for creating connections to RabbitMQ
ConnectionFactory factory = new();

// Location where RabbitMQ is running
//
// amqp://   -> RabbitMQ protocol
// guest     -> username
// guest     -> password
// localhost -> RabbitMQ server address
// 5672      -> RabbitMQ default port
factory.Uri = new Uri("amqp://guest:guest@localhost:5672");

// Human-readable connection name (visible in RabbitMQ dashboard/logs)
factory.ClientProvidedName = "Rabbit Sender App";

// Opens a real TCP connection to RabbitMQ server
IConnection cnn = await factory.CreateConnectionAsync();

// Lightweight communication stream inside a connection
// Best practice:
//     One connection -> many channels
IChannel channel = await cnn.CreateChannelAsync();

// Exchange receives messages from producers
// and decides where they should go
string exchangeName = "MQExchange";

// Routing key acts like an address/tag
// Exchange uses it to decide which queue
// should receive the message
string routingKey = "mq-routing-key";

// Queue stores messages until consumers read them
string queueName = "DMQQueue";

// Creates the exchange if it doesn't already exist
//
// ExchangeType.Direct means:
//     Messages are routed using exact routing key matching
await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct);

// Creates queue if it doesn't already exist
//
// Parameters:
//     durable    -> false = queue disappears after RabbitMQ restart
//     exclusive  -> false = multiple consumers can access queue
//     autoDelete -> false = queue won't delete automatically
await channel.QueueDeclareAsync(queueName, false, false, false, null);

// Connects:
//     Exchange + RoutingKey + Queue
//
// Meaning:
//     If exchange receives a message with
//     routingKey = "demo-routing-key"
//     then send it to "DemoQueue"
await channel.QueueBindAsync(queueName, exchangeName, routingKey, null);

// Optional message metadata
var props = new BasicProperties
{
    // Message content type
    ContentType = "text/plain"
};

// Send 60 messages
for (int i = 0; i < 60; i++)
{
    // Create message object
    MessageFormat data = new()
    {
        Id = i,
        Message = $"Message from: {System.Net.Dns.GetHostName()}",
        messageTimeStamp = $"Time of message: {DateTime.Now}",
        userName = $"Sender name: {Environment.UserName}",
        osVersion = $"OS: {Environment.OSVersion}"
    };

    // Calculate message size
    string json = JsonSerializer.Serialize(data);
    data.MessageSizeBytes = Encoding.UTF8.GetByteCount(json);

    // Serialize final version
    json = JsonSerializer.Serialize(data);

    Console.WriteLine($"Sending: {json}");

    // Convert JSON -> byte[]
    byte[] messageBodyBytes = Encoding.UTF8.GetBytes(json);

    // Send message
    await channel.BasicPublishAsync( exchangeName, routingKey, false,
        new BasicProperties
        {
            ContentType = "application/json"
        },
        messageBodyBytes);

    await Task.Delay(1000);
}

// Close channel
await channel.CloseAsync();

// Close TCP connection
await cnn.CloseAsync();
