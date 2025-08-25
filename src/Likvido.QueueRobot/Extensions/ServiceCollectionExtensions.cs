using System.Linq;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Likvido.QueueRobot.Extensions;

[PublicAPI]
public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddQueueRobot(this IServiceCollection services, Action<QueueRobotOptions> configureOptions)
    {
        // Create a temporary service provider to configure and access options
        var tempServices = new ServiceCollection();

        // Configure options in the temporary collection
        tempServices.AddOptions<QueueRobotOptions>()
            .Configure(configureOptions)
            .ValidateDataAnnotations();

        // Build the temporary provider
        using (var tempProvider = tempServices.BuildServiceProvider())
        {
            // Get the configured options
            var options = tempProvider.GetRequiredService<IOptions<QueueRobotOptions>>().Value;

            // Register handlers right away using the resolved options (distinct handler types)
            foreach (var handlerType in options.EventExecutors.Select(e => e.HandlerType).Distinct())
            {
                services.AddScoped(handlerType);
            }

            // Configure options in the main service collection
            services.AddOptions<QueueRobotOptions>()
                .Configure(configureOptions)
                .ValidateDataAnnotations();

            // Register the options for convenience
            services.AddSingleton(sp => sp.GetRequiredService<IOptions<QueueRobotOptions>>().Value);
        }

        return services;
    }
}
