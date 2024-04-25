using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Likvido.QueueRobot.Extensions;

[PublicAPI]
public static class ServiceCollectionExtensions
{
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
