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
            options.QueueName = configuration.GetValue<string>("QueueName");
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
            options.QueueName = configuration.GetValue<string>("QueueName");
            options.HighPriorityQueueName = configuration.GetValue<string>("HighPriorityQueueName");
            options.AzureStorageConnectionString = configuration.GetConnectionString("StorageConnectionString");
            options.AddMessageHandler<MyMessageHandler, MyMessage>();
        });
    });
```
