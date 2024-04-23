using System.Text;
using Azure.Storage.Queues.Models;

namespace Likvido.QueueRobot.MessageProcessing;

internal static class QueueMessageExtensions
{
    public static string GetMessageText(this QueueMessage message) =>
        Encoding.UTF8.GetString(Convert.FromBase64String(message.MessageText));
}
