// RECEIVER 2

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitShared;
using System.Text;
using System.Text.Json;

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

// Human-readable connection name
factory.ClientProvidedName = "Rabbit Receiver App";

// Opens a real TCP connection to RabbitMQ server
IConnection cnn = await factory.CreateConnectionAsync();

// Lightweight communication stream inside the connection
IChannel channel = await cnn.CreateChannelAsync();

// Exchange routes messages to queues
string exchangeName = "MQExchange";

// Routing key used for message routing
string routingKey = "mq-routing-key";

// Queue stores messages until consumers process them
string queueName = "MQQueue";

// Create exchange if it doesn't exist
await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct);

// Create queue if it doesn't exist
await channel.QueueDeclareAsync(queueName, false, false, false, null);

// Bind queue to exchange using routing key
//
// Meaning:
//     Messages sent to DemoExchange
//     with routing key "demo-routing-key"
//     should go into DemoQueue
await channel.QueueBindAsync(queueName, exchangeName, routingKey, null);

// QoS = Quality of Service
//
// prefetchCount = 1 means:
//
// RabbitMQ will only send ONE unacknowledged
// message at a time to this consumer.
//
// Prevents consumer overload and enables
// fair work distribution between consumers.
await channel.BasicQosAsync(0, 1, false);

// Create asynchronous consumer
var consumer = new AsyncEventingBasicConsumer(channel);

// Event triggered whenever a message is received
consumer.ReceivedAsync += async (sender, args) =>
{
    await Task.Delay(TimeSpan.FromSeconds(3));

    // Get raw bytes
    var body = args.Body.ToArray();

    // Convert bytes -> JSON string
    string json = Encoding.UTF8.GetString(body);

    Console.WriteLine($"Received JSON: {json}");

    // Convert JSON -> object
    MessageFormat? message = JsonSerializer.Deserialize<MessageFormat>(json);

    /*Console.WriteLine($"Id: {message?.Id}");
    Console.WriteLine($"Message: {message?.Message}");
    Console.WriteLine($"Message: {message?.timeStamp}");*/

    // ACK of the message (is taken out of the queue)
    await channel.BasicAckAsync(args.DeliveryTag, false);
};

// Start consuming messages from queue
//
// autoAck = false means:
//     We manually acknowledge messages
//
// This is safer because messages are not lost
// if consumer crashes during processing
string consumerTag = await channel.BasicConsumeAsync(queueName, false, consumer);

// Keep application running
Console.ReadLine();

// Stop consumer
await channel.BasicCancelAsync(consumerTag);

// Close channel
await channel.CloseAsync();

// Close TCP connection
await cnn.CloseAsync();
