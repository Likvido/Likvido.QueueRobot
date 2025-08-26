using System.Security.Principal;
using Likvido.Identity.PrincipalProviders;

namespace Likvido.QueueRobot.PrincipalProviders;

public class QueueMessagePrincipalProvider : IPrincipalProvider
{
    private static readonly AsyncLocal<IPrincipal?> Current = new();

    public IPrincipal? User => Current.Value;

    public bool IsSystemProvider => false;

    internal static void SetPrincipal(IPrincipal? principal)
    {
        Current.Value = principal;
    }
}
