using System.Text.Json;
using Likvido.CloudEvents;

namespace Likvido.QueueRobot.MessageProcessing.EventExecutors;

internal interface IEventExecutor
{
    string EventType { get; }
    Type HandlerType { get; }
    Task Execute(IServiceProvider serviceProvider, JsonElement jsonMessage, LikvidoPriority priority, bool lastAttempt, CancellationToken cancellationToken);
}
