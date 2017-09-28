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
using System.Linq;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.Spanner.EntityFrameworkCore.Storage.Internal
{
    /// <summary>
    /// </summary>
    public class SpannerTypeMapper : RelationalTypeMapper
    {
        private readonly StringTypeMapping _defaultString
            = new StringTypeMapping(SpannerDbType.String.DatabaseTypeName, DbType.String, true);
        private readonly LongTypeMapping _long = new LongTypeMapping(SpannerDbType.Int64.DatabaseTypeName, DbType.Int64);
        private readonly BoolTypeMapping _bool = new BoolTypeMapping(SpannerDbType.Bool.DatabaseTypeName);
        private readonly DateTimeTypeMapping _date = new DateTimeTypeMapping(SpannerDbType.Date.DatabaseTypeName, DbType.Date);
        private readonly DateTimeTypeMapping _datetime = new DateTimeTypeMapping(SpannerDbType.Timestamp.DatabaseTypeName, DbType.DateTime);
        private readonly DoubleTypeMapping _double = new DoubleTypeMapping(SpannerDbType.Float64.DatabaseTypeName);

        private readonly Dictionary<string, RelationalTypeMapping> _storeTypeMappings;
        private readonly Dictionary<System.Type, RelationalTypeMapping> _clrTypeMappings;

        /// <summary>
        /// </summary>
        public SpannerTypeMapper(RelationalTypeMapperDependencies dependencies)
            : base(dependencies)
        {
            _storeTypeMappings 
                = new Dictionary<string, RelationalTypeMapping>(StringComparer.OrdinalIgnoreCase)
                {
                    { _long.StoreType, _long },
                    { _bool.StoreType, _bool },
                    { _date.StoreType, _date },
                    { _datetime.StoreType, _datetime },
                    { _double.StoreType, _double },
                    { _defaultString.StoreType, _defaultString },
                };

            _clrTypeMappings
                = new Dictionary<System.Type, RelationalTypeMapping>
                {
                    { SpannerDbType.Int64.DefaultClrType, _long },
                    { SpannerDbType.Bool.DefaultClrType, _bool },
                    { SpannerDbType.Timestamp.DefaultClrType, _datetime },
                    { SpannerDbType.Float64.DefaultClrType, _double },
                    { SpannerDbType.String.DefaultClrType, _defaultString },
                };
        }

        /// <inheritdoc />
        protected override string GetColumnType(IProperty property)
            => new RelationalPropertyAnnotations(property).ColumnType;

        /// <inheritdoc />
        protected override IReadOnlyDictionary<System.Type, RelationalTypeMapping> GetClrTypeMappings()
            => _clrTypeMappings;

        /// <inheritdoc />
        protected override IReadOnlyDictionary<string, RelationalTypeMapping> GetStoreTypeMappings()
            => _storeTypeMappings;
    }
}
