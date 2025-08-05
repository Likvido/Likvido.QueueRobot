using JetBrains.Annotations;

namespace Likvido.QueueRobot.Exceptions;

[PublicAPI]
public enum PostponeStrategy
{
    Constant = 0,
    Linear = 1,
    Exponential = 2,
}