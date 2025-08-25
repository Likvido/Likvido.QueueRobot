using System.ComponentModel.DataAnnotations;
using System.Security.Claims;
using JetBrains.Annotations;
using Likvido.CloudEvents;
using Likvido.QueueRobot.MessageHandling;
using Likvido.QueueRobot.ValidationAttributes;
using System.Text.Json;
using Likvido.QueueRobot.JsonConverters;
using Likvido.QueueRobot.MessageProcessing;
using Likvido.QueueRobot.MessageProcessing.EventExecutors;
using Likvido.QueueRobot.PrincipalProviders;
using Microsoft.Extensions.DependencyInjection;

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
    internal List<IEventExecutor> EventExecutors { get; } = new();

    public QueueRobotOptions AddMessageHandler<TMessageHandler, TEvent>()
        where TMessageHandler : IMessageHandler<CloudEvent<TEvent>, TEvent>
    {
        return AddMessageHandler<TMessageHandler, TEvent>("*");
    }

    public QueueRobotOptions AddMessageHandler<TMessageHandler, TEvent>(string eventType)
        where TMessageHandler : IMessageHandler<CloudEvent<TEvent>, TEvent>
    {
        if (EventExecutors.Any(e => string.Equals(e.EventType, eventType, StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException($"Another message handler is already registered for the event type '{eventType}'");
        }

        EventExecutors.Add(new EventExecutor<TMessageHandler, TEvent>(eventType));

        return this;
    }
}
