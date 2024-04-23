using System.Reflection;
using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
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
        var processorName = Assembly.GetEntryAssembly()?.GetName().Name;
        using var logScope = logger.BeginScope("{Processor} reads {queueName}", processorName, options.QueueName);
        try
        {
            var queueClient = new QueueClient(options.AzureStorageConnectionString, options.QueueName);
            await queueClient.CreateIfNotExistsAsync(cancellationToken: CancellationToken.None);
            using var processor = new QueueMessageProcessor(
                        logger,
                        queueClient,
                        serviceProvider,
                        options,
                        telemetryClient);

            var queueMessageResponse = await queueClient.ReceiveMessagesAsync(1, options.VisibilityTimeout, cancellationToken);
            var queueMessage = queueMessageResponse?.Value.FirstOrDefault();
            if (queueMessage == null)
            {
                logger.LogInformation("No messages, quitting...");
                return;
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
    }
}
