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
using System.Data;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    /// </summary>
    public class SpannerTypeMapper : RelationalTypeMapper
    {
        private static readonly BoolTypeMapping s_bool
            = new BoolTypeMapping(SpannerDbType.Bool.DatabaseTypeName);
        private static readonly DateTimeTypeMapping s_date
            = new DateTimeTypeMapping(SpannerDbType.Date.DatabaseTypeName, DbType.DateTime);
        private static readonly DateTimeTypeMapping s_datetime
            = new DateTimeTypeMapping(SpannerDbType.Timestamp.DatabaseTypeName, DbType.DateTime);
        private static readonly StringTypeMapping s_defaultString
            = new StringTypeMapping(SpannerDbType.String.DatabaseTypeName, DbType.String, true);
        private static readonly DoubleTypeMapping s_double
            = new DoubleTypeMapping(SpannerDbType.Float64.DatabaseTypeName);
        private static readonly LongTypeMapping s_long 
            = new LongTypeMapping(SpannerDbType.Int64.DatabaseTypeName, DbType.Int64);

        private static readonly Dictionary<System.Type, RelationalTypeMapping> s_clrTypeMappings
            = new Dictionary<System.Type, RelationalTypeMapping>
        {
            {typeof(int), s_long },
            {typeof(long), s_long },
            {typeof(uint), s_long},
            {typeof(bool), s_bool},
            {typeof(DateTime), s_datetime},
            {typeof(float), s_double},
            {typeof(double), s_double},
            {typeof(string), s_defaultString}
        };

        private static readonly Dictionary<SpannerDbType, RelationalTypeMapping> s_dbTypeMappings
            = new Dictionary<SpannerDbType, RelationalTypeMapping>
            {
                { SpannerDbType.Bool, s_bool },
                { SpannerDbType.Bytes, null },
                { SpannerDbType.Date, s_date },
                { SpannerDbType.Float64, s_double },
                { SpannerDbType.Int64, s_long },
                { SpannerDbType.Timestamp, s_datetime },
                { SpannerDbType.String, s_defaultString },
                { SpannerDbType.Unspecified, null },
            };


        /// <summary>
        /// </summary>
        public SpannerTypeMapper(RelationalTypeMapperDependencies dependencies)
            : base(dependencies)
        {
        }

        /// <inheritdoc />
        protected override string GetColumnType(IProperty property)
            => new RelationalPropertyAnnotations(property).ColumnType;

        /// <inheritdoc />
        protected override IReadOnlyDictionary<System.Type, RelationalTypeMapping> GetClrTypeMappings()
            => s_clrTypeMappings;

        /// <inheritdoc />
        protected override IReadOnlyDictionary<string, RelationalTypeMapping> GetStoreTypeMappings()
            => null;

        /// <inheritdoc />
        protected override RelationalTypeMapping CreateMappingFromStoreType(string storeType)
        {
            if (SpannerDbType.TryParse(storeType, out SpannerDbType parsedType))
            {
                switch(parsedType.)
            }
        }
    }
}