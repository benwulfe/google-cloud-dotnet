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
using System.Linq.Expressions;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Remotion.Linq.Parsing.ExpressionVisitors.TreeEvaluation;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    class SpannerQueryCompiler : QueryCompiler
    {
        public SpannerQueryCompiler(IQueryContextFactory queryContextFactory,
            ICompiledQueryCache compiledQueryCache, 
            ICompiledQueryCacheKeyGenerator compiledQueryCacheKeyGenerator, 
            IDatabase database, IDiagnosticsLogger<DbLoggerCategory.Query> logger,
            INodeTypeProviderFactory nodeTypeProviderFactory, 
            ICurrentDbContext currentContext, 
            IEvaluatableExpressionFilter evaluatableExpressionFilter) 
            : base(queryContextFactory, compiledQueryCache, compiledQueryCacheKeyGenerator, 
                  database, logger, nodeTypeProviderFactory, currentContext, evaluatableExpressionFilter)
        {
        }

        protected override Expression ExtractParameters(Expression query, QueryContext queryContext, bool parameterize = true)
        {
            //workaround, if needed for b/67960412
            return base.ExtractParameters(query, queryContext, parameterize);
        }
    }
}
