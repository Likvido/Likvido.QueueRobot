namespace Likvido.QueueRobot.MessageHandling
{
    public interface IMessageHandlerBase
    {
        Task HandleMessage(object cloudEvent, bool lastAttempt, CancellationToken cancellationToken);
    }
}
