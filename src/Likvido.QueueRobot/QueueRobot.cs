using JetBrains.Annotations;
using Likvido.Robot;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Likvido.QueueRobot;

[PublicAPI]
public static class QueueRobot
{
    public static async Task Run(string name, Action<IConfiguration, IServiceCollection> configureServices) => 
        await RobotOperation.Run<QueueEngine>(name, configureServices);
}
