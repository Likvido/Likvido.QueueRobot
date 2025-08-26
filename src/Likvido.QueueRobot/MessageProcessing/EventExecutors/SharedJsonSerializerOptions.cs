using System.Text.Json;
using Likvido.QueueRobot.JsonConverters;

namespace Likvido.QueueRobot.MessageProcessing.EventExecutors;

internal static class SharedJsonSerializerOptions
{
    public static readonly JsonSerializerOptions Options;

    static SharedJsonSerializerOptions()
    {
        Options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            AllowTrailingCommas = true,
            ReadCommentHandling = JsonCommentHandling.Skip
        };
        Options.Converters.Add(new LikvidoPriorityConverter());
    }
}
