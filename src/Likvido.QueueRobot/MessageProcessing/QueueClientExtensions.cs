using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;

namespace Likvido.QueueRobot.MessageProcessing;

internal static class QueueClientExtensions
{
    public static async Task AddMessageAndCreateIfNotExistsAsync(
        this QueueClient queueClient,
        string message,
        TimeSpan? visibilityTimeout = null,
        CancellationToken cancellationToken = default)
    {
        if (queueClient == null)
        {
            throw new ArgumentNullException(nameof(queueClient));
        }

        try
        {
            await SendMessage(queueClient, message, visibilityTimeout, cancellationToken);
        }
        catch (RequestFailedException exception)
            when (exception.ErrorCode == QueueErrorCode.QueueNotFound
                  && exception.Status == 404)
        {
            await queueClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
            await SendMessage(queueClient, message, visibilityTimeout, cancellationToken);
        }
    }

    private static async Task SendMessage(QueueClient queueClient, string message, TimeSpan? visibilityTimeout, CancellationToken cancellationToken)
    {
        await queueClient.SendMessageAsync(
            message,
            visibilityTimeout,
            timeToLive: TimeSpan.FromSeconds(-1), // Using -1 means that the message does not expire.
            cancellationToken: cancellationToken);
    }
}
