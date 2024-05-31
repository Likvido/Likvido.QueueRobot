using JetBrains.Annotations;

namespace Likvido.QueueRobot.Exceptions;

[PublicAPI]
public class PostponeProcessingException(TimeSpan postponeTime) : Exception($"Postpone processing for {postponeTime}")
{
    public TimeSpan PostponeTime { get; } = postponeTime;
}
