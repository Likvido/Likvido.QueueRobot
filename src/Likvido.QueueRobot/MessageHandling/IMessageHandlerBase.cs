using Likvido.CloudEvents;

namespace Likvido.QueueRobot.MessageHandling
{
    public interface IMessageHandlerBase
    {
        Task HandleMessage(object cloudEvent,
            LikvidoPriority priority, bool lastAttempt, CancellationToken cancellationToken);
    }
}
