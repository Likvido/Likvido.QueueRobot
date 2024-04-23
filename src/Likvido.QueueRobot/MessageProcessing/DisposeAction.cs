namespace Likvido.QueueRobot.MessageProcessing;

internal sealed class DisposeAction : IAsyncDisposable
{
    private Func<Task>? _action;

    public DisposeAction(Func<Task> action)
    {
        _action = action;
    }

    public async ValueTask DisposeAsync()
    {
        if (_action == null)
        {
            return;
        }
        Func<Task>? localAction = null;
        lock (_action)
        {
            localAction = _action;
            _action = null;
        }

        if (localAction != null)
        {
            await localAction().ConfigureAwait(false);
        }
    }
}
