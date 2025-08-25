using System.Security.Claims;
using System.Text.Json;
using Likvido.CloudEvents;
using Likvido.QueueRobot.MessageHandling;
using Likvido.QueueRobot.PrincipalProviders;
using Microsoft.Extensions.DependencyInjection;

namespace Likvido.QueueRobot.MessageProcessing.EventExecutors;

internal sealed class EventExecutor<TMessageHandler, TEvent>(string eventType) : IEventExecutor
    where TMessageHandler : IMessageHandler<CloudEvent<TEvent>, TEvent>
{
    public string EventType { get; } = eventType;
    public Type HandlerType => typeof(TMessageHandler);

    public async Task Execute(IServiceProvider serviceProvider, JsonElement jsonMessage, LikvidoPriority priority, bool lastAttempt, CancellationToken cancellationToken)
    {
        var message = jsonMessage.Deserialize<CloudEvent<TEvent>>(SharedJsonSerializerOptions.Options)!;
        if (priority == LikvidoPriority.High)
        {
            // If the message was read from the high-priority queue, we need to make sure the priority is set to high
            message.LikvidoPriority = priority;
        }

        QueueMessagePrincipalProvider.SetPrincipal(GetPrincipalFromMessage(message));
        try
        {
            var handler = serviceProvider.GetRequiredService<TMessageHandler>();
            await handler.HandleMessage(message, lastAttempt, cancellationToken);
        }
        finally
        {
            // Ensure principal is cleared after handler completes to avoid leaking to other messages
            QueueMessagePrincipalProvider.SetPrincipal(null);
        }
    }

    private static ClaimsPrincipal GetPrincipalFromMessage(CloudEvent<TEvent> message) =>
        message.LikvidoUserClaims is null
            ? new ClaimsPrincipal(new ClaimsIdentity())
            : new ClaimsPrincipal(new ClaimsIdentity(
                message.LikvidoUserClaims.Select(x => new Claim(x.Key, x.Value)).ToList(),
                authenticationType: "QueueMessage"));
}
