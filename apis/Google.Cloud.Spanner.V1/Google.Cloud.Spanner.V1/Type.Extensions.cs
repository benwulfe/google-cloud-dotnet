// Copyright 2017 Google Inc. All Rights Reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;

namespace Google.Cloud.Spanner.V1
{
    /// <summary>
    /// </summary>
    public static class TypeCodeExtensions
    {
        private static readonly Dictionary<string, TypeCode> s_originalNameToCode
            = new Dictionary<string, TypeCode>
            {
                {TypeCode.Bool.ToString().ToUpperInvariant(), TypeCode.Bool},
                {TypeCode.Unspecified.ToString().ToUpperInvariant(), TypeCode.Unspecified},
                {TypeCode.Int64.ToString().ToUpperInvariant(), TypeCode.Int64},
                {TypeCode.Float64.ToString().ToUpperInvariant(), TypeCode.Float64},
                {TypeCode.Timestamp.ToString().ToUpperInvariant(), TypeCode.Timestamp},
                {TypeCode.Date.ToString().ToUpperInvariant(), TypeCode.Date},
                {TypeCode.String.ToString().ToUpperInvariant(), TypeCode.String},
                {TypeCode.Bytes.ToString().ToUpperInvariant(), TypeCode.Bytes},
                {TypeCode.Array.ToString().ToUpperInvariant(), TypeCode.Array},
                {TypeCode.Struct.ToString().ToUpperInvariant(), TypeCode.Struct},
            };

        /// <summary>
        /// </summary>
        /// <param name="typeCode"></param>
        /// <returns></returns>
        public static string GetOriginalName(this TypeCode typeCode)
        {
            return typeCode.ToString().ToUpperInvariant();
        }

        /// <summary>
        /// Returns a TypeCode given its original string representation.
        /// </summary>
        /// <param name="originalName"></param>
        /// <returns></returns>
        public static TypeCode GetTypeCode(string originalName) 
            => s_originalNameToCode.TryGetValue(originalName.Trim(), out var typeCode) 
                    ? typeCode : TypeCode.Unspecified;

        /// <summary>
        /// Parses a subsection of the given string into a TypeCode and size, returning the nested content
        /// as a remainder
        /// </summary>
        /// <param name="complexName">A full string type representation such as ARRAY{STRING(5)}</param>
        /// <param name="typeCode">The <see cref="TypeCode"/> of the outer type represented by the string.</param>
        /// <param name="size">The size of the returned type if specified</param>
        /// <param name="remainder">The remaining string to be parsed. This will be either the elementtype
        /// for arrays or struct members for a struct.</param>
        /// <returns></returns>
        public static bool TryParse(string complexName, out TypeCode typeCode, out int? size, out string remainder)
        {
            typeCode = TypeCode.Unspecified;
            size = null;
            remainder = null;
            if (string.IsNullOrEmpty(complexName))
            {
                return false;
            }

            int remainderStart = complexName.IndexOfAny(new[] {'<', '('});
            remainderStart = remainderStart != -1 ? remainderStart : complexName.Length - 1;
            typeCode = GetTypeCode(complexName.Substring(0, remainderStart));
            if (typeCode == TypeCode.Unspecified)
            {
                return false;
            }
            if (complexName.Length < remainderStart)
            {
                if (complexName[remainderStart] == '(')
                {
                    //get the size and remainder to send back
                    var sizeEnd = complexName.IndexOf(')');
                    if (sizeEnd == -1)
                    {
                        return false;
                    }
                    if (int.TryParse(complexName.Substring(remainderStart + 1, sizeEnd - remainderStart - 1).Trim(), 
                                        out int parsedSize))
                    {
                        size = parsedSize;
                    }
                    remainder = complexName.Substring(sizeEnd + 1);
                }
                else
                {
                    var innerEnd = complexName.LastIndexOf('>');
                    if (innerEnd == -1)
                    {
                        return false;
                    }
                    //get the remainder to send back.
                    remainder = complexName.Substring(remainderStart, innerEnd - remainderStart - 1);
                }
            }
            return true;

        }
    }
}
