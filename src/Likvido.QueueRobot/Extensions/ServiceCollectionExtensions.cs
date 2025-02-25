using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Likvido.QueueRobot.Extensions;

[PublicAPI]
public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddQueueRobot(this IServiceCollection services, Action<QueueRobotOptions> configureOptions)
    {
        services.AddOptions<QueueRobotOptions>()
            .Configure(configureOptions)
            .ValidateDataAnnotations()
            .PostConfigure(options =>
            {
                foreach (var mapping in options.EventTypeHandlerDictionary)
                {
                    services.AddScoped(mapping.Value.HandlerType);
                }
            });

        // Register the options for convenience, to avoid having to resolve IOptions<QueueRobotOptions>
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<QueueRobotOptions>>().Value);

        return services;
    }
}
