using System.ComponentModel.DataAnnotations;
using JetBrains.Annotations;
using Likvido.CloudEvents;
using Likvido.QueueRobot.MessageHandling;
using Likvido.QueueRobot.ValidationAttributes;

namespace Likvido.QueueRobot;

[PublicAPI]
public class QueueRobotOptions
{
    [Required]
    public required string AzureStorageConnectionString { get; set; }

    /// <summary>
    /// The name of the queue to read messages from
    /// </summary>
    [Required]
    public required string QueueName { get; set; }

    /// <summary>
    /// Optional name of the queue to read high priority messages from (messages from this queue is read before the normal queue)
    /// </summary>
    public string? HighPriorityQueueName { get; set; }

    /// <summary>
    /// The maximum number of times a message will be retried
    /// Default is 5
    /// </summary>
    public int MaxRetryCount { get; set; } = 5;

    /// <summary>
    /// The time the message is invisible in the queue after it has been read
    /// Default 30 seconds which is default for QueueClient.ReceiveMessagesAsync
    /// </summary>
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromSeconds(30);

    [RequiredCollection]
    internal IDictionary<string, (Type MessageType, Type HandlerType)> EventTypeHandlerDictionary { get; } = new Dictionary<string, (Type, Type)>(StringComparer.OrdinalIgnoreCase);

    public QueueRobotOptions AddMessageHandler<TMessageHandler, TEvent>()
        where TMessageHandler : IMessageHandler<CloudEvent<TEvent>, TEvent>
    {
        return AddMessageHandler<TMessageHandler, TEvent>("*");
    }

    public QueueRobotOptions AddMessageHandler<TMessageHandler, TEvent>(string eventType)
        where TMessageHandler : IMessageHandler<CloudEvent<TEvent>, TEvent>
    {
        if (EventTypeHandlerDictionary.ContainsKey(eventType))
        {
            throw new InvalidOperationException($"Another message handler is already registered for the event type '{eventType}'");
        }

        EventTypeHandlerDictionary[eventType] = (typeof(TEvent), typeof(TMessageHandler));
        return this;
    }
}
