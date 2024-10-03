using System.Text.Json;
using System.Text.Json.Serialization;
using Likvido.CloudEvents;

namespace Likvido.QueueRobot.JsonConverters;

public class LikvidoPriorityConverter : JsonConverter<LikvidoPriority?>
{
    public override LikvidoPriority? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.Null:
                return null;
            case JsonTokenType.String:
            {
                if (Enum.TryParse<LikvidoPriority>(reader.GetString(), true, out var result))
                {
                    return result;
                }

                break;
            }
            case JsonTokenType.Number when reader.TryGetInt32(out int intValue):
                return (LikvidoPriority)intValue;
        }

        throw new JsonException($"Unable to convert \"{reader.GetString()}\" to {nameof(LikvidoPriority)}.");
    }

    public override void Write(Utf8JsonWriter writer, LikvidoPriority? value, JsonSerializerOptions options)
    {
        if (value.HasValue)
        {
            writer.WriteStringValue(value.Value.ToString());
        }
        else
        {
            writer.WriteNullValue();
        }
    }
}
