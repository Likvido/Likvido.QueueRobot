# Likvido.QueueRobot
Helper library for creating queue based robots in Likvido

The idea is that the robot will be controlled by KEDA, and will run as a Job in Kubernetes. The robot will be able to scale up and down based on the number of messages in the queue.

You can have several message handlers in your robot, in case you are able to process different types of messages.

Each handler needs to implement the `IMessageHandler<in TCloudEvent, TMessage>` interface. To make it a bit easier to do, we have provided a small helper base class - but using that is not required.

```csharp
public class MyMessageHandler : MessageHandlerBase<MyMessage>
{
    public override Task HandleMessage(CloudEvent<MyMessage> cloudEvent, bool lastAttempt, CancellationToken cancellationToken)
    {
        // Your robot code here
    }
}
```

You then register your handlers during the setup of the robot:

```csharp
await QueueRobot.Run(
    "my-robot",
    (configuration, services) =>
    {
        services.AddQueueRobot(options =>
        {
            options.QueueName = configuration.GetRequiredValue("QueueName");
            options.AzureStorageConnectionString = configuration.GetConnectionString("StorageConnectionString");
            options.AddMessageHandler<MyMessageHandler, MyMessage>();
        });
    });
```

You can optionally add a "high priority" queue, which will be processed before the regular queue:

```csharp
await QueueRobot.Run(
    "my-robot",
    (configuration, services) =>
    {
        services.AddQueueRobot(options =>
        {
            options.QueueName = configuration.GetRequiredValue("QueueName");
            options.HighPriorityQueueName = configuration.GetRequiredValue("HighPriorityQueueName");
            options.AzureStorageConnectionString = configuration.GetConnectionString("StorageConnectionString");
            options.AddMessageHandler<MyMessageHandler, MyMessage>();
        });
    });
```

## Postpone processing a message

Under some circumstances, you might wish to postpone the processing of a given message for some time period. Maybe calling some dependency fails, and you wish to delay the retry for some extended period of time (if you only want to delay for seconds, then you could consider just using Task.Delay). To do this, we have added a special exception you can throw, called `PostponeProcessingException`. With this exception, you can supply a TimeSpan, which indicates how long you wish to postpone processing the message. The library will then update the visibility timeout on the message, so that it will be invisible for the time period you specified. Note that the max time period allowed for postponing a message is 7 days (this is a limitation in Azure Storage Queues)
