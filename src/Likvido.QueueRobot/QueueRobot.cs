using Likvido.Robot;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Likvido.QueueRobot;

public static class QueueRobot
{
    public static async Task Run(string name, Action<IConfiguration, IServiceCollection> configureServices) => 
        await RobotOperation.Run<QueueEngine>(name, configureServices);

    public static IServiceCollection AddQueueRobot(this IServiceCollection services, Action<QueueRobotOptions> configureOptions)
    {
        services.AddOptions<QueueRobotOptions>().Configure(configureOptions).ValidateDataAnnotations();
        RegisterMessageHandlers(services);

        return services;
    }

    private static void RegisterMessageHandlers(IServiceCollection services)
    {
        var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<IOptions<QueueRobotOptions>>().Value;

        foreach (var mapping in options.EventTypeHandlerDictionary)
        {
            services.AddScoped(mapping.Value.HandlerType);
        }

        services.AddSingleton(resolver => options);
    }
}
