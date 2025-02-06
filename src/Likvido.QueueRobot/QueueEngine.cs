using System.Reflection;
using Azure.Storage.Queues;
using JetBrains.Annotations;
using Likvido.CloudEvents;
using Likvido.QueueRobot.MessageProcessing;
using Likvido.Robot;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Likvido.QueueRobot;

[UsedImplicitly]
public class QueueEngine(
    ILogger<QueueEngine> logger,
    IServiceProvider serviceProvider,
    QueueRobotOptions options) : ILikvidoRobotEngine
{
    public async Task Run(CancellationToken cancellationToken)
    {
        AssertAllMessageHandlersCanBeConstructed();

        for (var i = 0; i < options.MaxMessagesToProcess; i++)
        {
            var messageProcessed = false;
            if (!string.IsNullOrWhiteSpace(options.HighPriorityQueueName))
            {
                messageProcessed = await ProcessMessageFromQueue(options.HighPriorityQueueName, LikvidoPriority.High, cancellationToken);
            }

            if (!messageProcessed)
            {
                messageProcessed = await ProcessMessageFromQueue(options.QueueName, LikvidoPriority.Normal, cancellationToken);
            }

            if (!messageProcessed)
            {
                logger.LogInformation("No messages in any queue, stopping...");
                break;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogInformation("Cancellation requested, stopping...");
                break;
            }
        }
    }

    private void AssertAllMessageHandlersCanBeConstructed()
    {
        foreach (var (_, handlerType) in options.EventTypeHandlerDictionary.Values)
        {
            serviceProvider.GetRequiredService(handlerType);
        }
    }

    private async Task<bool> ProcessMessageFromQueue(string queueName, LikvidoPriority priority, CancellationToken cancellationToken)
    {
        var processorName = Assembly.GetEntryAssembly()?.GetName().Name;
        using var logScope = logger.BeginScope("{Processor} reads {QueueName} (priority: {Priority}", processorName, queueName, Enum.GetName(priority));
        try
        {
            var queueClient = new QueueClient(options.AzureStorageConnectionString, queueName);
            await queueClient.CreateIfNotExistsAsync(cancellationToken: CancellationToken.None);
            using var processor = new QueueMessageProcessor(
                logger,
                queueClient,
                serviceProvider,
                options,
                queueName);

            var queueMessageResponse = await queueClient.ReceiveMessagesAsync(1, options.VisibilityTimeout, cancellationToken);
            var queueMessage = queueMessageResponse?.Value.FirstOrDefault();
            if (queueMessage == null)
            {
                logger.LogInformation("No messages in {QueueName}...", queueName);
                return false;
            }

            await processor.ProcessMessage(queueMessage, priority, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unhandled exception");
            throw;
        }

        return true;
    }
}
