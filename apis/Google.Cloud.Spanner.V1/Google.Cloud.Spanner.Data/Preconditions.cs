using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner
{

    internal static class Preconditions
    {
        public static void Precondition(this object argument, string nameofArg, params object[] values)
        {
            Precondition(argument, nameofArg, "", values);
        }

        public static void Precondition(this object argument, string nameofArg, string detail, params object[] values)
        {
            if (values == null || !values.Any(x => Equals(argument, x)))
            {
                throw new ArgumentException($"Invalid value for {nameofArg}. {detail}");
            }
        }

        public static void Precondition<T>(this T thisT, Predicate<T> argument, string message)
        {
            if (!argument(thisT))
            {
                throw new ArgumentException(message);
            }
        }
    }
}
