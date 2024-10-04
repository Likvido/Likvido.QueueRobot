using Likvido.CloudEvents;

namespace Likvido.QueueRobot.MessageHandling
{
    public abstract class MessageHandlerBase<TEvent> : IMessageHandler<CloudEvent<TEvent>, TEvent>
    {
        public abstract Task HandleMessage(CloudEvent<TEvent> cloudEvent, bool lastAttempt, CancellationToken cancellationToken);

        Task IMessageHandlerBase.HandleMessage(object cloudEventObject, LikvidoPriority priority, bool lastAttempt, CancellationToken cancellationToken)
        {
            var cloudEvent = (CloudEvent<TEvent>)cloudEventObject;
            if (priority == LikvidoPriority.High)
            {
                // If the message was read from the high-priority queue, we need to make sure the priority is set to high
                cloudEvent.LikvidoPriority = priority;
            }

            return HandleMessage(cloudEvent, lastAttempt, cancellationToken);
        }
    }
}
