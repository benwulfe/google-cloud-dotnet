using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.Spanner
{

    internal static class Preconditions
    {
        public static void AssertOneOf(this object argument, string nameofArg, params object[] values)
        {
            AssertOneOf(argument, nameofArg, "", values);
        }

        public static void AssertOneOf(this object argument, string nameofArg, string detail, params object[] values)
        {
            if (values == null || !values.Any(x => Equals(argument, x)))
            {
                throw new ArgumentException( $"Invalid value for {nameofArg}. {detail}");
            }
        }

        public static void AssertTrue<T>(this T thisT, Predicate<T> argument, string message)
        {
            if (!argument(thisT))
            {
                throw new ArgumentException(message);
            }
        }

        public static void AssertNotNull<T>(this T thisT, string variableName)
        {
            AssertTrue(thisT, t => t != null, $"{variableName} is not allowed to be null");
        }

        public static void AssertNotNullOrEmpty(this string thisT, string variableName)
        {
            AssertTrue(thisT, t => !string.IsNullOrEmpty(t), $"{variableName} is not allowed to be null or empty");
        }
    }
}
