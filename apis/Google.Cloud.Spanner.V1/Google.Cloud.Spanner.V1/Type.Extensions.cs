using System;
using System.Collections.Generic;
using System.Text;

namespace Google.Cloud.Spanner.V1
{
    /// <summary>
    /// </summary>
    public static class TypeCodeExtensions
    {
        /// <summary>
        /// </summary>
        /// <param name="typeCode"></param>
        /// <returns></returns>
        public static string GetOriginalName(this TypeCode typeCode)
        {
            switch (typeCode)
            {
                case TypeCode.Unspecified:
                    return "UKNOWN";
                case TypeCode.Bool:
                    return "BOOL";
                case TypeCode.Int64:
                    return "INT64";
                case TypeCode.Float64:
                    return "FLOAT64";
                case TypeCode.Timestamp:
                    return "TIMESTAMP";
                case TypeCode.Date:
                    return "DATE";
                case TypeCode.String:
                    return "STRING";
                case TypeCode.Bytes:
                    return "BYTES";
                case TypeCode.Array:
                    return "ARRAY";
                case TypeCode.Struct:
                    return "STRUCT";
                default:
                    throw new ArgumentOutOfRangeException(nameof(typeCode), typeCode, null);
            }
        }
    }
}
