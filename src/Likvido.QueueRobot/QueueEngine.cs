using System.Reflection;
using Azure.Storage.Queues;
using JetBrains.Annotations;
using Likvido.QueueRobot.MessageProcessing;
using Likvido.Robot;
using Microsoft.ApplicationInsights;
using Microsoft.Extensions.Logging;

namespace Likvido.QueueRobot;

[UsedImplicitly]
public class QueueEngine(
    ILogger<QueueEngine> logger,
    IServiceProvider serviceProvider,
    TelemetryClient telemetryClient,
    QueueRobotOptions options) : ILikvidoRobotEngine
{
    public async Task Run(CancellationToken cancellationToken)
    {
        var processedHighPriorityMessage = false;
        if (!string.IsNullOrWhiteSpace(options.HighPriorityQueueName))
        {
            processedHighPriorityMessage = await ProcessMessageFromQueue(options.HighPriorityQueueName, cancellationToken);
        }

        if (!processedHighPriorityMessage)
        {
            await ProcessMessageFromQueue(options.QueueName, cancellationToken);
        }
    }

    private async Task<bool> ProcessMessageFromQueue(string queueName, CancellationToken cancellationToken)
    {
        var processorName = Assembly.GetEntryAssembly()?.GetName().Name;
        using var logScope = logger.BeginScope("{Processor} reads {QueueName}", processorName, queueName);
        try
        {
            var queueClient = new QueueClient(options.AzureStorageConnectionString, queueName);
            await queueClient.CreateIfNotExistsAsync(cancellationToken: CancellationToken.None);
            using var processor = new QueueMessageProcessor(
                logger,
                queueClient,
                serviceProvider,
                options,
                telemetryClient,
                queueName);

            var queueMessageResponse = await queueClient.ReceiveMessagesAsync(1, options.VisibilityTimeout, cancellationToken);
            var queueMessage = queueMessageResponse?.Value.FirstOrDefault();
            if (queueMessage == null)
            {
                logger.LogInformation("No messages in {QueueName}...", queueName);
                return false;
            }

            await processor.ProcessMessage(queueMessage, cancellationToken);

            telemetryClient.Flush();
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
