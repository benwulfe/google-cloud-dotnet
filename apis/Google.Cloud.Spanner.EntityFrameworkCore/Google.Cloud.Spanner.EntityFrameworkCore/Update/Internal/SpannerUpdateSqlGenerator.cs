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

using System.Text;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace Google.Cloud.Spanner.EntityFrameworkCore.Update.Internal
{
    /// <summary>
    /// 
    /// </summary>
    public class SpannerUpdateSqlGenerator : UpdateSqlGenerator
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dependencies"></param>
        /// <param name="typeMapper"></param>
        public SpannerUpdateSqlGenerator(
            UpdateSqlGeneratorDependencies dependencies,
            IRelationalTypeMapper typeMapper)
            : base(dependencies)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="commandStringBuilder"></param>
        /// <param name="expectedRowsAffected"></param>
        protected override void AppendRowsAffectedWhereCondition(StringBuilder commandStringBuilder, int expectedRowsAffected)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="commandStringBuilder"></param>
        /// <param name="columnModification"></param>
        protected override void AppendIdentityWhereCondition(StringBuilder commandStringBuilder, ColumnModification columnModification)
        {
        }
    }
}
