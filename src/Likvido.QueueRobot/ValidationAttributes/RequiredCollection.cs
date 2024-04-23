using System.Collections;
using System.ComponentModel.DataAnnotations;

namespace Likvido.QueueRobot.ValidationAttributes;

public class RequiredCollection : ValidationAttribute
{
    public override bool IsValid(object? value)
    {
        if (value is ICollection list)
        {
            return list.Count > 0;
        }

        return false;
    }

    public override string FormatErrorMessage(string name)
    {
        return $"The {name} collection must contain at least one element.";
    }
}
