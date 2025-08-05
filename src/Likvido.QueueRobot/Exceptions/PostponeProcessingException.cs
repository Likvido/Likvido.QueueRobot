using JetBrains.Annotations;

namespace Likvido.QueueRobot.Exceptions;

[PublicAPI]
public class PostponeProcessingException(TimeSpan postponeTime) : PostponeProcessingStrategyException(PostponeStrategy.Constant, postponeTime)
{
}
