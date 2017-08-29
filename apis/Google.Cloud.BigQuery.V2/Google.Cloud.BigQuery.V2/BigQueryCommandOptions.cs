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
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.BigQuery.V2
{
    /// <summary>
    /// </summary>
    public abstract class BigQueryCommandOptions
    {
        internal BigQueryConnection BigQueryConnection { get; set; }

        internal string CommandText { get; set; }

        /// <summary>
        /// </summary>
        public GetQueryResultsOptions GetQueryResultsOptions { get; set; }

        internal T As<T>() where T : class
        {
            if (typeof(T) != GetType())
            {
                throw new InvalidOperationException(
                    $"To perform this command, you must set options to an instance of {nameof(T)}");
            }
            return this as T;
        }

        internal abstract Func<BigQueryCommand, CancellationToken, Task<BigQueryJob>> CreateBigQueryJobFunc { get; }
    }
}
