using JetBrains.Annotations;

namespace Likvido.QueueRobot.Exceptions;

[PublicAPI]
public class PostponeProcessingStrategyException : Exception
{
    private readonly PostponeStrategy _strategy;

    private readonly TimeSpan _baseDelay;

    public PostponeProcessingStrategyException(PostponeStrategy strategy, TimeSpan baseDelay) : base($"Postpone processing for {baseDelay} with strategy {strategy}")
    {
        _strategy = strategy;
        _baseDelay = baseDelay;
    }

    public TimeSpan PostponeTime(long attemptNumber)
    {
        // Safeguard to ensure this is never negative or 0
        attemptNumber = attemptNumber <= 0 ? 1 : attemptNumber;
        return _strategy switch
        {
            PostponeStrategy.Constant => _baseDelay,
            PostponeStrategy.Linear => TimeSpan.FromMilliseconds(_baseDelay.TotalMilliseconds * attemptNumber),
            PostponeStrategy.Exponential => TimeSpan.FromMilliseconds(_baseDelay.TotalMilliseconds * Math.Pow(2, attemptNumber - 1)),
            _ => _baseDelay
        };
    }
}