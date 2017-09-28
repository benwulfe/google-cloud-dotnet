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

using Google.Api.Gax;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace Google.Cloud.Spanner.EntityFrameworkCore.Update.Internal
{
    /// <summary>
    /// 
    /// </summary>
    public class SpannerModificationCommandBatchFactory : IModificationCommandBatchFactory
    {
        private readonly IRelationalCommandBuilderFactory _commandBuilderFactory;
        private readonly ISqlGenerationHelper _sqlGenerationHelper;
        private readonly IUpdateSqlGenerator _updateSqlGenerator;
        private readonly IRelationalValueBufferFactoryFactory _valueBufferFactoryFactory;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="commandBuilderFactory"></param>
        /// <param name="sqlGenerationHelper"></param>
        /// <param name="updateSqlGenerator"></param>
        /// <param name="valueBufferFactoryFactory"></param>
        /// <param name="options"></param>
        public SpannerModificationCommandBatchFactory(
            IRelationalCommandBuilderFactory commandBuilderFactory,
            ISqlGenerationHelper sqlGenerationHelper,
            IUpdateSqlGenerator updateSqlGenerator,
            IRelationalValueBufferFactoryFactory valueBufferFactoryFactory,
            IDbContextOptions options)
        {
            GaxPreconditions.CheckNotNull(commandBuilderFactory, nameof(commandBuilderFactory));
            GaxPreconditions.CheckNotNull(sqlGenerationHelper, nameof(sqlGenerationHelper));
            GaxPreconditions.CheckNotNull(updateSqlGenerator, nameof(updateSqlGenerator));
            GaxPreconditions.CheckNotNull(valueBufferFactoryFactory, nameof(valueBufferFactoryFactory));
            GaxPreconditions.CheckNotNull(options, nameof(options));

            _commandBuilderFactory = commandBuilderFactory;
            _sqlGenerationHelper = sqlGenerationHelper;
            _updateSqlGenerator = updateSqlGenerator;
            _valueBufferFactoryFactory = valueBufferFactoryFactory;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public virtual ModificationCommandBatch Create()
            => new SingularModificationCommandBatch(
                _commandBuilderFactory,
                _sqlGenerationHelper,
                _updateSqlGenerator,
                _valueBufferFactoryFactory);
    }
}
