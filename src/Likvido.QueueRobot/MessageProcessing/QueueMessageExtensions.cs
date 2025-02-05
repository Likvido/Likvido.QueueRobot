using System.Text;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Logging;

namespace Likvido.QueueRobot.MessageProcessing;

internal static class QueueMessageExtensions
{
    public static string GetMessageText(this QueueMessage message, ILogger logger)
    {
        try
        {
            if (string.IsNullOrEmpty(message.MessageText))
            {
                logger.LogError("Queue message text is null or empty");
                throw new ArgumentException("Message text is null or empty");
            }

            byte[] bytes = Convert.FromBase64String(message.MessageText);
            return Encoding.UTF8.GetString(bytes);
        }
        catch (Exception ex)
        {
            const int truncateLength = 300;
            logger.LogError(ex, "Failed to decode base64 string. Length: {Length}, First " + truncateLength + " chars: {MessagePreview}",
                message.MessageText?.Length,
                message.MessageText?.Substring(0, Math.Min(message.MessageText?.Length ?? 0, truncateLength)));

            throw;
        }
    }
}