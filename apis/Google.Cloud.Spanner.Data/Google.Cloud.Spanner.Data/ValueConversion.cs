﻿// Copyright 2017 Google Inc. All Rights Reserved.
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
using System.Collections;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Xml;
using Google.Protobuf.WellKnownTypes;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;
using System.Reflection;

namespace Google.Cloud.Spanner.Data
{
    internal static class ValueConversion
    {
        public static object ConvertToClrType(this Value wireValue, V1.Type spannerType)
        {
            return ConvertToClrType(wireValue, spannerType, spannerType?.Code.GetDefaultClrTypeFromSpannerType());
        }

        public static T ConvertToClrType<T>(this Value wireValue,
            V1.Type spannerType)
        {
            return (T)ConvertToClrType(wireValue, spannerType, typeof(T));
        }

        public static object ConvertToClrType(this Value wireValue, V1.Type spannerType, System.Type targetClrType)
        {
            if (wireValue.KindCase == Value.KindOneofCase.NullValue)
            {
                if (targetClrType.GetTypeInfo().IsValueType)
                {
                    //return default(T)
                    return Activator.CreateInstance(targetClrType);
                }
                return null;
            }
            var possibleUnderlyingType = Nullable.GetUnderlyingType(targetClrType);
            if (possibleUnderlyingType != null)
            {
                targetClrType = possibleUnderlyingType;
            }
            //extra supported conversions that are modifications of the "core" versions but may have loss of precision.
            //we call the spannerType with the known supported version and cast it down to lose precision.
            if (targetClrType == typeof(int))
                return Convert.ToInt32((long)ConvertToClrTypeImpl(wireValue, spannerType, typeof(long)));
            if (targetClrType == typeof(uint))
                return Convert.ToUInt32((long)ConvertToClrTypeImpl(wireValue, spannerType, typeof(long)));
            if (targetClrType == typeof(short))
                return Convert.ToInt16((long)ConvertToClrTypeImpl(wireValue, spannerType, typeof(long)));
            if (targetClrType == typeof(ushort))
                return Convert.ToUInt16((long)ConvertToClrTypeImpl(wireValue, spannerType, typeof(long)));
            if (targetClrType == typeof(char))
                return Convert.ToChar((long)ConvertToClrTypeImpl(wireValue, spannerType, typeof(long)));
            if (targetClrType == typeof(byte))
                return Convert.ToByte((long)ConvertToClrTypeImpl(wireValue, spannerType, typeof(long)));
            if (targetClrType == typeof(decimal))
                return Convert.ToDecimal((long)ConvertToClrTypeImpl(wireValue, spannerType, typeof(decimal)));
            if (targetClrType == typeof(float))
                return Convert.ToSingle((double)ConvertToClrTypeImpl(wireValue, spannerType, typeof(double)));
            if (targetClrType == typeof(Guid))
                return Guid.Parse((string)ConvertToClrTypeImpl(wireValue, spannerType, typeof(string)));
            return ConvertToClrTypeImpl(wireValue, spannerType, targetClrType);
        }

        public static DbType GetDbType(this TypeCode spannerType)
        {
            switch (spannerType)
            {
                case TypeCode.Bool:
                    return DbType.Boolean;
                case TypeCode.Int64:
                    return DbType.Int64;
                case TypeCode.Float64:
                    return DbType.Double;
                case TypeCode.Timestamp:
                    return DbType.DateTime;
                case TypeCode.Date:
                    return DbType.Date;
                case TypeCode.String:
                    return DbType.String;
                case TypeCode.Bytes:
                    return DbType.Binary;
                case TypeCode.Unspecified:
                case TypeCode.Array:
                case TypeCode.Struct:
                    return DbType.Object;
                default:
                    throw new ArgumentOutOfRangeException(nameof(spannerType), spannerType, null);
            }
        }

        public static System.Type GetDefaultClrTypeFromSpannerType(this TypeCode code)
        {
            switch (code)
            {
                case TypeCode.Bool:
                    return typeof(bool?);
                case TypeCode.Int64:
                    return typeof(long?);
                case TypeCode.Float64:
                    return typeof(double?);
                case TypeCode.Timestamp:
                case TypeCode.Date:
                    return typeof(DateTime?);
                case TypeCode.String:
                    return typeof(string);
                case TypeCode.Bytes:
                    return typeof(byte[]);
                case TypeCode.Array:
                    return typeof(ArrayList);
                case TypeCode.Struct:
                    return typeof(Hashtable);
                default:
                    //if we don't recognize it (or its a struct), we use the google native wellknown type.
                    return typeof(Value);
            }
        }

        public static TypeCode GetSpannerTypeCode(this SpannerDbType spannerDbType)
        {
            return spannerDbType.TypeCode;
        }

        public static Value ToValue(object value, SpannerDbType spannerDbType)
        {
            if (value == null)
            {
                return Value.ForNull();
            }

            switch (spannerDbType.TypeCode)
            {
                case TypeCode.Bytes:
                    var s = value as string;
                    if (s != null)
                    {
                        return new Value { StringValue = s };
                    }
                    else if (value is byte[])
                    {
                        return new Value { StringValue = Convert.ToBase64String((byte[])value) };
                    }
                    throw new ArgumentOutOfRangeException(nameof(value), value,
                        "TypeCode.Bytes only supports string and byte[]");
                case TypeCode.Bool:
                    return new Value { BoolValue = Convert.ToBoolean(value) };
                case TypeCode.String:
                case TypeCode.Int64:
                    return new Value { StringValue = value.ToString() };
                case TypeCode.Float64:
                    return new Value { NumberValue = Convert.ToDouble(value) };
                case TypeCode.Timestamp:
                    var stringValue = value as string;
                    if (stringValue != null)
                        return new Value { StringValue = stringValue };
                    return new Value
                    {
                        StringValue = XmlConvert.ToString(Convert.ToDateTime(value), XmlDateTimeSerializationMode.Utc)
                    };
                case TypeCode.Date:
                    stringValue = value as string;
                    if (stringValue != null)
                        return new Value { StringValue = stringValue };
                    return new Value
                    {
                        StringValue = StripTimePart(
                            XmlConvert.ToString(Convert.ToDateTime(value), XmlDateTimeSerializationMode.Utc))
                    };
                case TypeCode.Array:
                    IEnumerable enumerable = value as IEnumerable;
                    if (enumerable != null)
                    {
                        return Value.ForList(enumerable.Cast<object>()
                            .Select(x => ToValue(x, spannerDbType.ArrayElementType)).ToArray());
                    }
                    throw new ArgumentOutOfRangeException(nameof(spannerDbType), spannerDbType, "The given array instance needs to implement IEnumerable.");
                case TypeCode.Struct:
                    IDictionary dictionary = value as IDictionary;
                    if (dictionary != null)
                    {
                        Struct structValue = new Struct();
                        foreach (var key in dictionary.Keys)
                        {
                            string keyString = key.ToString();
                            if (!structValue.Fields.ContainsKey(keyString))
                            {
                                throw new ArgumentOutOfRangeException(nameof(value), value,
                                    "The given struct instance has members not defined in the Struct.");
                            }
                            structValue.Fields[keyString] = ToValue(dictionary[key],
                                spannerDbType.StructMembers[keyString]);
                        }
                        return Value.ForStruct(structValue);
                    }
                    throw new ArgumentOutOfRangeException(nameof(spannerDbType), spannerDbType, "The given struct instance needs to implement IDictionary.");

                default:
                    throw new ArgumentOutOfRangeException(nameof(spannerDbType), spannerDbType, null);
            }
        }

        private static object ConvertToClrTypeImpl(Value wireValue, V1.Type spannerType, System.Type targetClrType)
        {
            //If the wireValue itself is assignable to the target type, just return it
            //This covers both typeof(Value) and typeof(object).
            if (wireValue == null || targetClrType == null || targetClrType == typeof(Value))
                return wireValue;

            //targetClrType should be one of the values returned by GetDefaultClrTypeFromSpannerType
            if (targetClrType == typeof(bool))
                switch (wireValue.KindCase)
                {
                    case Value.KindOneofCase.NullValue:
                        return default(bool);
                    case Value.KindOneofCase.StringValue:
                        return Convert.ToBoolean(wireValue.StringValue);
                    case Value.KindOneofCase.BoolValue:
                        return wireValue.BoolValue;
                    default:
                        throw new InvalidOperationException(
                            $"Invalid Type conversion from {wireValue.KindCase} to {targetClrType.FullName}");
                }
            if (targetClrType == typeof(long))
                switch (wireValue.KindCase)
                {
                    case Value.KindOneofCase.NumberValue:
                        return Convert.ToInt64(wireValue.NumberValue);
                    case Value.KindOneofCase.NullValue:
                        return default(long);
                    case Value.KindOneofCase.StringValue:
                        return Convert.ToInt64(wireValue.StringValue);
                    default:
                        throw new InvalidOperationException(
                            $"Invalid Type conversion from {wireValue.KindCase} to {targetClrType.FullName}");
                }
            if (targetClrType == typeof(double))
                switch (wireValue.KindCase)
                {
                    case Value.KindOneofCase.NullValue:
                        return default(double);
                    case Value.KindOneofCase.NumberValue:
                        return wireValue.NumberValue;
                    case Value.KindOneofCase.StringValue:
                        if (string.Compare(wireValue.StringValue, "NaN", StringComparison.OrdinalIgnoreCase) == 0)
                            return double.NaN;
                        if (string.Compare(wireValue.StringValue, "Infinity", StringComparison.OrdinalIgnoreCase) == 0)
                            return double.PositiveInfinity;
                        if (string.Compare(wireValue.StringValue, "-Infinity", StringComparison.OrdinalIgnoreCase) == 0)
                            return double.NegativeInfinity;

                        return Convert.ToDouble(wireValue.StringValue);
                    default:
                        throw new InvalidOperationException(
                            $"Invalid Type conversion from {wireValue.KindCase} to {targetClrType.FullName}");
                }
            if (targetClrType == typeof(DateTime))
                switch (wireValue.KindCase)
                {
                    case Value.KindOneofCase.NullValue:
                        return null;
                    case Value.KindOneofCase.StringValue:
                        return XmlConvert.ToDateTime(wireValue.StringValue, XmlDateTimeSerializationMode.Utc);
                    default:
                        throw new InvalidOperationException(
                            $"Invalid Type conversion from {wireValue.KindCase} to {targetClrType.FullName}");
                }
            if (targetClrType == typeof(Timestamp))
                switch (wireValue.KindCase)
                {
                    case Value.KindOneofCase.NullValue:
                        return null;
                    case Value.KindOneofCase.StringValue:
                        return Timestamp.Parser.ParseJson(wireValue.StringValue);
                    default:
                        throw new InvalidOperationException(
                            $"Invalid Type conversion from {wireValue.KindCase} to {targetClrType.FullName}");
                }
            if (targetClrType == typeof(string))
                switch (wireValue.KindCase)
                {
                    case Value.KindOneofCase.NullValue:
                        return null;
                    case Value.KindOneofCase.NumberValue:
                        return wireValue.NumberValue.ToString(CultureInfo.CurrentCulture);
                    case Value.KindOneofCase.StringValue:
                        return wireValue.StringValue;
                    case Value.KindOneofCase.BoolValue:
                        return wireValue.BoolValue.ToString();
                    default:
                        return wireValue.ToString();
                }
            if (targetClrType == typeof(byte[]))
                switch (wireValue.KindCase)
                {
                    case Value.KindOneofCase.NullValue:
                        return null;
                    case Value.KindOneofCase.StringValue:
                        return Convert.FromBase64String(wireValue.StringValue);
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            if (targetClrType == typeof(ArrayList) || targetClrType == typeof(IList))
            {
                switch (wireValue.KindCase)
                {
                    case Value.KindOneofCase.NullValue: return null;
                    case Value.KindOneofCase.ListValue:
                        return new ArrayList(wireValue.ListValue.Values
                            .Select(x => x.ConvertToClrType(spannerType.ArrayElementType))
                            .ToList());
                    default:
                        throw new InvalidOperationException(
                            $"Invalid Type conversion from {wireValue.KindCase} to {targetClrType.FullName}");
                }
            }
            if (targetClrType == typeof(Hashtable) ||
                targetClrType == typeof(IDictionary))
            {
                //a bit of recursion here...
                Hashtable dictionary;
                switch (wireValue.KindCase)
                {
                    case Value.KindOneofCase.StructValue:
                        dictionary = new Hashtable();
                        foreach (var structField in spannerType.StructType.Fields)
                            dictionary[structField.Name] =
                                wireValue.StructValue.Fields[structField.Name]
                                    .ConvertToClrType(structField.Type);
                        return dictionary;
                    case Value.KindOneofCase.ListValue:
                        dictionary = new Hashtable();
                        if (spannerType.Code == TypeCode.Struct)
                        {
                            for (int i = 0; i < spannerType.StructType.Fields.Count; i++)
                            {
                                dictionary[spannerType.StructType.Fields[i].Name] = wireValue
                                    .ListValue.Values[i]
                                    .ConvertToClrType(spannerType.StructType.Fields[i].Type);
                            }
                        }
                        else
                        {
                            var i = 0;
                            foreach (var listItemValue in wireValue.ListValue.Values)
                            {
                                dictionary[i] =
                                    listItemValue.ConvertToClrType(spannerType.ArrayElementType);
                                i++;
                            }
                        }
                        return dictionary;
                    default:
                        throw new ArgumentException(
                            $"Invalid Type conversion from {wireValue.KindCase} to {targetClrType.FullName}");
                }
            }
            if (targetClrType.IsArray)
            {
                switch (wireValue.KindCase)
                {
                    case Value.KindOneofCase.NullValue: return null;
                    case Value.KindOneofCase.ListValue:
                        Array newArray = Array.CreateInstance(targetClrType.GetElementType(),
                            wireValue.ListValue.Values.Count);

                        int i = 0;
                        foreach (var obj in wireValue.ListValue.Values.Select(
                            x => x.ConvertToClrType(spannerType.ArrayElementType, targetClrType.GetElementType())))
                        {
                            newArray.SetValue(obj, i);
                            i++;
                        }
                        return newArray;
                    default:
                        throw new InvalidOperationException(
                            $"Invalid Type conversion from {wireValue.KindCase} to {targetClrType.FullName}");
                }

            }
            if (typeof(IList).IsAssignableFrom(targetClrType))
            {
                switch (wireValue.KindCase)
                {
                    case Value.KindOneofCase.NullValue: return null;
                    case Value.KindOneofCase.ListValue:
                        IList newList = (IList)Activator.CreateInstance(targetClrType);
                        foreach (var obj in wireValue.ListValue.Values.Select(
                            x => x.ConvertToClrType(spannerType.ArrayElementType)))
                        {
                            newList.Add(obj);
                        }
                        return newList;
                    default:
                        throw new InvalidOperationException(
                            $"Invalid Type conversion from {wireValue.KindCase} to {targetClrType.FullName}");
                }
            }
            throw new ArgumentException(
                $"Invalid Type conversion from {wireValue.KindCase} to {targetClrType.FullName}");
        }

        private static string StripTimePart(string rfc3339String)
        {
            if (!string.IsNullOrEmpty(rfc3339String))
            {
                var timeIndex = rfc3339String.IndexOf('T');
                if (timeIndex != -1)
                    return rfc3339String.Substring(0, timeIndex);
            }
            return rfc3339String;
        }
    }
}