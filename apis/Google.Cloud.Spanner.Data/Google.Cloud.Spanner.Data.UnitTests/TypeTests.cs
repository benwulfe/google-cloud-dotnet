using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Xunit;

namespace Google.Cloud.Spanner.Data.Tests
{
    //more exhaustive (compared to integration tests) type conversion test cases.
    public class TypeTests
    {
        public enum TestType
        {
            //Only test the CLR type -> Value conversion
            ClrToValue = 0,

            //Only test the Value -> CLR Type conversion
            ValueToClr = 1,

            //Test both ways.  There can be no loss of information or precision in the ClrToValue conversion.
            Both = 2
        }

        private static readonly DateTime s_testDate = new DateTime(2017, 1, 31, 3, 15, 30, 500);
        private static readonly byte[] s_bytesToEncode = {1, 2, 3, 4};
        private static readonly string s_base64Encoded = Convert.ToBase64String(s_bytesToEncode);

        private static readonly SpannerDbType s_struct = SpannerDbType.StructOf(
            new Dictionary<string, SpannerDbType>
            {
                {"StringField", SpannerDbType.String},
                {"Int64Field", SpannerDbType.Int64},
                {"Float64Field", SpannerDbType.Float64},
                {"BoolField", SpannerDbType.Bool},
                {"DateField", SpannerDbType.Date},
                {"TimestampField", SpannerDbType.Timestamp}
            });

        //nested complex type support.
        private static readonly SpannerDbType s_arrayOfStruct = SpannerDbType.ArrayOf(s_struct);

        private static readonly SpannerDbType s_complexStruct = SpannerDbType.StructOf(
            new Dictionary<string, SpannerDbType>
            {
                {"StructField", s_struct},
                {"ArrayField", SpannerDbType.ArrayOf(SpannerDbType.Int64)}
            });

        private static string S(string s) => $"\"{s}\"";

        private static IEnumerable<string> GetStringsForArray()
        {
            yield return "abc";
            yield return "123";
            yield return "def";
        }

        private static IEnumerable<int> GetIntsForArray()
        {
            yield return 4;
            yield return 5;
            yield return 6;
        }

        private static IEnumerable<double> GetFloatsForArray()
        {
            yield return 1.0;
            yield return 2.0;
            yield return 3.0;
        }

        private static IEnumerable<bool> GetBoolsForArray()
        {
            yield return true;
            yield return false;
            yield return true;
        }

        private static IEnumerable<DateTime> GetDatesForArray()
        {
            yield return new DateTime(2017, 1, 31);
            yield return new DateTime(2016, 2, 15);
            yield return new DateTime(2015, 3, 31);
        }

        private static IEnumerable<DateTime> GetTimestampsForArray()
        {
            yield return new DateTime(2017, 1, 31, 3, 15, 30, 500);
            yield return new DateTime(2016, 2, 15, 13, 15, 30, 000);
            yield return new DateTime(2015, 3, 31, 3, 15, 30, 250);
        }

        // This data serves as inputs to converting from CLR types that a developer
        // would use in his app to a given json serialized format using the spanner type
        // to determine how to serialize.
        // The inputs are used to test both ways (also deseralizing the generated json
        // to a requested clr type).  However some cases are specified as only one direction
        // usually because the conversion is by definition lossy.
        public static IEnumerable<object[]> GetValidValueConversions()
        {
            //format is:  LocalClrInstance,  SpannerType,  SerializedJsonFromProto, [test one or both ways]
            //testing can be one way if there is loss of information in the conversion.

            //Spanner type = Float64 tests.
            yield return new object[] {true, SpannerDbType.Float64, "1"};
            yield return new object[] {false, SpannerDbType.Float64, "0"};
            yield return new object[] {(byte) 1, SpannerDbType.Float64, "1"};
            yield return new object[] {(sbyte) 1, SpannerDbType.Float64, "1"};
            yield return new object[] {1.5M, SpannerDbType.Float64, "1.5"};
            yield return new object[] {1.5D, SpannerDbType.Float64, "1.5"};
            yield return new object[] {1.5F, SpannerDbType.Float64, "1.5"};
            yield return new object[] {double.NegativeInfinity, SpannerDbType.Float64, S("-Infinity")};
            yield return new object[] {double.PositiveInfinity, SpannerDbType.Float64, S("Infinity")};
            yield return new object[] {double.NaN, SpannerDbType.Float64, S("NaN")};
            yield return new object[] {1, SpannerDbType.Float64, "1"};
            yield return new object[] {1U, SpannerDbType.Float64, "1"};
            yield return new object[] {1L, SpannerDbType.Float64, "1"};
            yield return new object[] {(ulong) 1, SpannerDbType.Float64, "1"};
            yield return new object[] {(short) 1, SpannerDbType.Float64, "1"};
            yield return new object[] {(ushort) 1, SpannerDbType.Float64, "1"};
            yield return new object[] {"1", SpannerDbType.Float64, "1"};
            yield return new object[] {"1.5", SpannerDbType.Float64, "1.5"};
            yield return new object[] {null, SpannerDbType.Float64, "null"};

            //Spanner type = Int64 tests.
            yield return new object[] {true, SpannerDbType.Int64, S("1")};
            yield return new object[] {false, SpannerDbType.Int64, S("0")};
            yield return new object[] {(char) 1, SpannerDbType.Int64, S("1")};
            yield return new object[] {(byte) 1, SpannerDbType.Int64, S("1")};
            yield return new object[] {(sbyte) 1, SpannerDbType.Int64, S("1")};
            yield return new object[] {2M, SpannerDbType.Int64, S("2")};
            yield return new object[] {2D, SpannerDbType.Int64, S("2")};
            yield return new object[] {2F, SpannerDbType.Int64, S("2")};
            yield return new object[] {1, SpannerDbType.Int64, S("1")};
            yield return new object[] {1U, SpannerDbType.Int64, S("1")};
            yield return new object[] {1L, SpannerDbType.Int64, S("1")};
            yield return new object[] {(ulong) 1, SpannerDbType.Int64, S("1")};
            yield return new object[] {(short) 1, SpannerDbType.Int64, S("1")};
            yield return new object[] {(ushort) 1, SpannerDbType.Int64, S("1")};
            yield return new object[] {"1", SpannerDbType.Int64, S("1")};

            //Spanner type = Bool tests.
            yield return new object[] {true, SpannerDbType.Bool, "true"};
            yield return new object[] {false, SpannerDbType.Bool, "false"};
            yield return new object[] {(byte) 1, SpannerDbType.Bool, "true"};
            yield return new object[] {(byte) 0, SpannerDbType.Bool, "false"};
            yield return new object[] {(sbyte) 1, SpannerDbType.Bool, "true"};
            yield return new object[] {(sbyte) 0, SpannerDbType.Bool, "false"};
            yield return new object[] {1M, SpannerDbType.Bool, "true"};
            yield return new object[] {0M, SpannerDbType.Bool, "false"};
            yield return new object[] {1D, SpannerDbType.Bool, "true"};
            yield return new object[] {0D, SpannerDbType.Bool, "false"};
            yield return new object[] {1F, SpannerDbType.Bool, "true"};
            yield return new object[] {0F, SpannerDbType.Bool, "false"};
            yield return new object[] {1, SpannerDbType.Bool, "true"};
            yield return new object[] {0, SpannerDbType.Bool, "false"};
            yield return new object[] {1U, SpannerDbType.Bool, "true"};
            yield return new object[] {0U, SpannerDbType.Bool, "false"};
            yield return new object[] {1L, SpannerDbType.Bool, "true"};
            yield return new object[] {0L, SpannerDbType.Bool, "false"};
            yield return new object[] {(ulong) 1, SpannerDbType.Bool, "true"};
            yield return new object[] {(ulong) 0, SpannerDbType.Bool, "false"};
            yield return new object[] {(short) 1, SpannerDbType.Bool, "true"};
            yield return new object[] {(short) 0, SpannerDbType.Bool, "false"};
            yield return new object[] {(ushort) 1, SpannerDbType.Bool, "true"};
            yield return new object[] {(ushort) 0, SpannerDbType.Bool, "false"};

            //Spanner type = String tests.
            //Note the casing on bool->string follows c# bool conversion semantics (by design).
            yield return new object[] {true, SpannerDbType.String, S("True")};
            yield return new object[] {false, SpannerDbType.String, S("False")};
            yield return new object[] {(char) 26, SpannerDbType.String, S("\\u001a")};
            yield return new object[] {(byte) 1, SpannerDbType.String, S("1")};
            yield return new object[] {(sbyte) 1, SpannerDbType.String, S("1")};
            yield return new object[] {1.5M, SpannerDbType.String, S("1.5")};
            yield return new object[] {1.5D, SpannerDbType.String, S("1.5")};
            yield return new object[] {1.5F, SpannerDbType.String, S("1.5")};
            yield return new object[] {1, SpannerDbType.String, S("1")};
            yield return new object[] {1U, SpannerDbType.String, S("1")};
            yield return new object[] {1L, SpannerDbType.String, S("1")};
            yield return new object[] {(ulong) 1, SpannerDbType.String, S("1")};
            yield return new object[] {(short) 1, SpannerDbType.String, S("1")};
            yield return new object[] {(ushort) 1, SpannerDbType.String, S("1")};
            yield return new object[] {s_testDate, SpannerDbType.String, S("2017-01-31T03:15:30.5Z")};
            //Note the difference in C# conversions from special doubles.
            yield return new object[] {double.NegativeInfinity, SpannerDbType.String, S("-\u221E")};
            yield return new object[] {double.PositiveInfinity, SpannerDbType.String, S("\u221E")};
            yield return new object[] {double.NaN, SpannerDbType.String, S("NaN")};
            yield return new object[] {"1.5", SpannerDbType.String, S("1.5")};
            yield return new object[]
                {new ToStringClass("hello"), SpannerDbType.String, S("hello"), TestType.ClrToValue};

            //Spanner type = Date+Timestamp tests.  Some of these are one way due to either a lossy conversion (date loses time)
            // or a string formatting difference.
            yield return new object[] {s_testDate, SpannerDbType.Date, S("2017-01-31"), TestType.ClrToValue};
            yield return new object[] {"1/31/2017", SpannerDbType.Date, S("2017-01-31"), TestType.ClrToValue};
            yield return new object[]
                {"1/31/2017 3:15:30 AM", SpannerDbType.Date, S("2017-01-31"), TestType.ClrToValue};
            yield return new object[] {s_testDate, SpannerDbType.Timestamp, S("2017-01-31T03:15:30.5Z")};
            yield return new object[]
                {"1/31/2017", SpannerDbType.Timestamp, S("2017-01-31T00:00:00Z"), TestType.ClrToValue};
            yield return new object[]
                {"1/31/2017 3:15:30 AM", SpannerDbType.Timestamp, S("2017-01-31T03:15:30Z"), TestType.ClrToValue};

            //Spanner type = Bytes tests.
            yield return new object[] {s_base64Encoded, SpannerDbType.Bytes, S(s_base64Encoded)};
            yield return new object[] {s_bytesToEncode, SpannerDbType.Bytes, S(s_base64Encoded)};
            yield return new object[] {"passthrubadbytes", SpannerDbType.Bytes, S("passthrubadbytes")};

            //list test cases (list of type X).
            yield return new object[]
            {
                new List<string>(GetStringsForArray()), SpannerDbType.ArrayOf(SpannerDbType.String),
                "[ \"abc\", \"123\", \"def\" ]"
            };
            yield return new object[]
            {
                new List<double>(GetFloatsForArray()), SpannerDbType.ArrayOf(SpannerDbType.Float64),
                "[ 1, 2, 3 ]"
            };
            yield return new object[]
            {
                new List<int>(GetIntsForArray()), SpannerDbType.ArrayOf(SpannerDbType.Int64),
                "[ \"4\", \"5\", \"6\" ]"
            };
            yield return new object[]
            {
                new List<bool>(GetBoolsForArray()), SpannerDbType.ArrayOf(SpannerDbType.Bool),
                "[ true, false, true ]"
            };
            yield return new object[]
            {
                new List<DateTime>(GetDatesForArray()), SpannerDbType.ArrayOf(SpannerDbType.Date),
                "[ \"2017-01-31\", \"2016-02-15\", \"2015-03-31\" ]"
            };
            yield return new object[]
            {
                new List<DateTime>(GetTimestampsForArray()), SpannerDbType.ArrayOf(SpannerDbType.Timestamp),
                "[ \"2017-01-31T03:15:30.5Z\", \"2016-02-15T13:15:30Z\", \"2015-03-31T03:15:30.25Z\" ]"
            };

            //list test cases (various source/target list types)
            yield return new object[]
            {
                GetStringsForArray(), SpannerDbType.ArrayOf(SpannerDbType.String),
                "[ \"abc\", \"123\", \"def\" ]", TestType.ClrToValue
            };
            yield return new object[]
            {
                new ArrayList(GetStringsForArray().ToList()), SpannerDbType.ArrayOf(SpannerDbType.String),
                "[ \"abc\", \"123\", \"def\" ]"
            };
            yield return new object[]
            {
                new List<object>(GetStringsForArray()), SpannerDbType.ArrayOf(SpannerDbType.String),
                "[ \"abc\", \"123\", \"def\" ]"
            };
            yield return new object[]
            {
                new CustomList(GetStringsForArray()), SpannerDbType.ArrayOf(SpannerDbType.String),
                "[ \"abc\", \"123\", \"def\" ]"
            };

            //struct test case includes nested complex conversions.
            var sampleStruct = new Dictionary<string, object>
            {
                {"StringField", "stringValue"},
                {"Int64Field", 2L},
                {"Float64Field", double.NaN},
                {"BoolField", true},
                {"DateField", new DateTime(2017, 1, 31)},
                {"TimestampField", new DateTime(2017, 1, 31, 3, 15, 30)}
            };
            string sampleValueSerialized = "{ \"StringField\": \"stringValue\", \"Int64Field\": \"2\", "
                + "\"Float64Field\": \"NaN\", \"BoolField\": true, \"DateField\": \"2017-01-31\", "
                + "\"TimestampField\": \"2017-01-31T03:15:30Z\" }";

            yield return new object[]
            {
                sampleStruct,
                s_struct,
                sampleValueSerialized
            };
            yield return new object[]
            {
                new Hashtable(sampleStruct),
                s_struct, sampleValueSerialized
            };
            yield return new object[]
            {
                new CustomDictionary(sampleStruct),
                s_struct, sampleValueSerialized
            };

            //array of structs.
            yield return new object[]
            {
                new List<object>(new[] {sampleStruct}),
                s_arrayOfStruct, $"[ {sampleValueSerialized} ]"
            };

            //struct of struct+array.
            var complexStruct = new Dictionary<string, object>
            {
                {"StructField", new Hashtable(sampleStruct)},
                {"ArrayField", new ArrayList(GetIntsForArray().Select(x => (long) x).ToList())}
            };
            yield return new object[]
            {
                complexStruct, s_complexStruct,
                "{ \"StructField\": { \"StringField\": \"stringValue\", \"Int64Field\": \"2\", \"Float64Field\": \"NaN\", \"BoolField\": true, \"DateField\": \"2017-01-31\", \"TimestampField\": \"2017-01-31T03:15:30Z\" }, \"ArrayField\": [ \"4\", \"5\", \"6\" ] }"
            };
        }

        public static IEnumerable<object[]> GetInvalidValueConversions()
        {
            //Spanner type = Float64 tests.
            yield return new object[] {(char) 1, SpannerDbType.Float64, ""};
            yield return new object[] {s_testDate, SpannerDbType.Float64, ""};
            yield return new object[] {new ToStringClass("1.5"), SpannerDbType.Float64, ""};
            yield return new object[] {"", SpannerDbType.Float64, ""};

            //Spanner type = Int64 tests.
            yield return new object[] {s_testDate, SpannerDbType.Int64, ""};
            yield return new object[] {double.NegativeInfinity, SpannerDbType.Int64, ""};
            yield return new object[] {double.PositiveInfinity, SpannerDbType.Int64, ""};
            yield return new object[] {double.NaN, SpannerDbType.Int64, ""};
            yield return new object[] {"1.5", SpannerDbType.Int64, S("2")};
            yield return new object[] {new ToStringClass("1.5"), SpannerDbType.Int64, ""};

            //Spanner type = Bool tests.
            yield return new object[] {(char) 1, SpannerDbType.Bool, ""};
            yield return new object[] {"1", SpannerDbType.Bool, ""};
            yield return new object[] {new ToStringClass("true"), SpannerDbType.Bool, ""};

            //Spanner type = String tests.
            //(all work)

            //Spanner type = Date tests.
            yield return new object[] {new ToStringClass("hello"), SpannerDbType.Date, ""};
            yield return new object[] {"badjuju", SpannerDbType.Date, ""};
        }

        private string StrippedString(string s) => new StringBuilder(s)
            .Replace("{", "")
            .Replace("}", "")
            .Replace("[", "")
            .Replace("]", "")
            .Replace(" ", "")
            .Replace("\"StructField\":", "")
            .ToString();

        private bool IsEqualWhenDecomposed(string expected, string actual)
        {
            //There are cases where the serialization (of structs) is nondeterministic
            //This would cause the unit tests to fail randomly, so if we see a failure
            //we decompose the expected and actual into their respective pieces and validate
            //each piece.
            //we try to decompose the actual and expected in case its an unordered dictionary.
            expected = StrippedString(expected);
            actual = StrippedString(actual);
            var unorderedMatchExpected = new HashSet<string>(expected.Split(','));
            var unorderedMatchActual = new HashSet<string>(actual.Split(','));
            bool equal = unorderedMatchActual.Count == unorderedMatchExpected.Count;
            if (equal)
            {
                foreach (string s in unorderedMatchActual)
                {
                    equal &= unorderedMatchExpected.Contains(s);
                    if (!equal)
                    {
                        break;
                    }
                }
            }
            return equal;
        }

        [Theory]
        [MemberData(nameof(GetValidValueConversions))]
        public void TestSerializeToValue(
            object clrValue,
            SpannerDbType spannerDbType,
            string expectedJsonValue,
            TestType testType = TestType.Both)
        {
            if (testType == TestType.ValueToClr)
            {
                return;
            }
            string infoAddendum = $", type:{clrValue?.GetType().Name}, spannerType:{spannerDbType} ";
            try
            {
                string expected = expectedJsonValue;
                string actual = ValueConversion.ToValue(clrValue, spannerDbType).ToString();
                if (expected != actual)
                {
                    if (!IsEqualWhenDecomposed(expected, actual))
                    {
                        //our error message contains an informational addendum
                        //which tells us which theory test case failed.
                        Assert.Equal(expected + infoAddendum, actual + infoAddendum);
                    }
                }
            }
            catch (Exception e)
            {
                Assert.True(false, infoAddendum + e.Message);
                throw;
            }
        }

        [Theory]
        [MemberData(nameof(GetValidValueConversions))]
        public void TestDeSerializeFromValue(
            object expected,
            SpannerDbType spannerDbType,
            string inputJson,
            TestType testType = TestType.Both)
        {
            if (testType == TestType.ClrToValue)
            {
                return;
            }
            string infoAddendum = $"type:{expected?.GetType().Name}, spannerType:{spannerDbType}, input:{inputJson} ";
            try
            {
                var wireValue = JsonParser.Default.Parse<Value>(inputJson);
                var targetClrType = expected?.GetType() ?? typeof(object);
                var actual = wireValue.ConvertToClrType(spannerDbType.ToProtobufType(), targetClrType);
                Assert.Equal(expected, actual);
            }
            catch (Exception e)
            {
                Assert.True(false, infoAddendum + e);
                throw;
            }
        }

        [Theory]
        [MemberData(nameof(GetInvalidValueConversions))]
        public void TestInvalidSerializeToValue(
            object value,
            SpannerDbType type,
            string expectedJsonValue,
            TestType testType = TestType.Both)
        {
            if (testType == TestType.ValueToClr)
            {
                return;
            }
            string infoAddendum = $"type:{value?.GetType().Name}, spannerType:{type}";

            var exceptionCaught = false;
            try
            {
                ValueConversion.ToValue(value, type);
            }
            catch (Exception e) when (e is OverflowException || e is InvalidCastException || e is FormatException)
            {
                exceptionCaught = true;
            }
            Assert.True(exceptionCaught, infoAddendum);
        }

        private class ToStringClass
        {
            private readonly string _valueForToString;

            public ToStringClass(string valueForToString) => _valueForToString = valueForToString;

            /// <inheritdoc />
            public override string ToString() => _valueForToString;
        }

        private class CustomList : IList
        {
            private readonly IList _listImplementation = new ArrayList();

            public CustomList(IEnumerable contents) => _listImplementation =
                new ArrayList(contents.Cast<object>().ToList());

            // Used by ValueConversion via reflection upon deserialization.
            // ReSharper disable once UnusedMember.Local
            public CustomList() { }

            /// <inheritdoc />
            public IEnumerator GetEnumerator() => _listImplementation.GetEnumerator();

            /// <inheritdoc />
            public void CopyTo(Array array, int index)
            {
                _listImplementation.CopyTo(array, index);
            }

            /// <inheritdoc />
            public int Count => _listImplementation.Count;

            /// <inheritdoc />
            public bool IsSynchronized => _listImplementation.IsSynchronized;

            /// <inheritdoc />
            public object SyncRoot => _listImplementation.SyncRoot;

            /// <inheritdoc />
            public int Add(object value) => _listImplementation.Add(value);

            /// <inheritdoc />
            public void Clear()
            {
                _listImplementation.Clear();
            }

            /// <inheritdoc />
            public bool Contains(object value) => _listImplementation.Contains(value);

            /// <inheritdoc />
            public int IndexOf(object value) => _listImplementation.IndexOf(value);

            /// <inheritdoc />
            public void Insert(int index, object value)
            {
                _listImplementation.Insert(index, value);
            }

            /// <inheritdoc />
            public void Remove(object value)
            {
                _listImplementation.Remove(value);
            }

            /// <inheritdoc />
            public void RemoveAt(int index)
            {
                _listImplementation.RemoveAt(index);
            }

            /// <inheritdoc />
            public bool IsFixedSize => _listImplementation.IsFixedSize;

            /// <inheritdoc />
            public bool IsReadOnly => _listImplementation.IsReadOnly;

            /// <inheritdoc />
            public object this[int index]
            {
                get => _listImplementation[index];
                set => _listImplementation[index] = value;
            }
        }

        private class CustomDictionary : IDictionary
        {
            private readonly IDictionary _dictionaryImplementation = new Hashtable();

            // Used by ValueConversion via reflection upon deserialization.
            // ReSharper disable once UnusedMember.Local
            public CustomDictionary() { }

            public CustomDictionary(IDictionary contents) => _dictionaryImplementation = new Hashtable(contents);

            /// <inheritdoc />
            public void Add(object key, object value)
            {
                _dictionaryImplementation.Add(key, value);
            }

            /// <inheritdoc />
            public void Clear()
            {
                _dictionaryImplementation.Clear();
            }

            /// <inheritdoc />
            public bool Contains(object key) => _dictionaryImplementation.Contains(key);

            /// <inheritdoc />
            public IDictionaryEnumerator GetEnumerator() => _dictionaryImplementation.GetEnumerator();

            /// <inheritdoc />
            public void Remove(object key)
            {
                _dictionaryImplementation.Remove(key);
            }

            /// <inheritdoc />
            public bool IsFixedSize => _dictionaryImplementation.IsFixedSize;

            /// <inheritdoc />
            public bool IsReadOnly => _dictionaryImplementation.IsReadOnly;

            /// <inheritdoc />
            public object this[object key]
            {
                get => _dictionaryImplementation[key];
                set => _dictionaryImplementation[key] = value;
            }

            /// <inheritdoc />
            public ICollection Keys => _dictionaryImplementation.Keys;

            /// <inheritdoc />
            public ICollection Values => _dictionaryImplementation.Values;

            /// <inheritdoc />
            IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable) _dictionaryImplementation).GetEnumerator();

            /// <inheritdoc />
            public void CopyTo(Array array, int index)
            {
                _dictionaryImplementation.CopyTo(array, index);
            }

            /// <inheritdoc />
            public int Count => _dictionaryImplementation.Count;

            /// <inheritdoc />
            public bool IsSynchronized => _dictionaryImplementation.IsSynchronized;

            /// <inheritdoc />
            public object SyncRoot => _dictionaryImplementation.SyncRoot;
        }
    }
}
