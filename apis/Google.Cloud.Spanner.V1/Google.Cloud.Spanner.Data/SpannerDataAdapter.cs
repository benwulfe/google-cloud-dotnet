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

// ReSharper disable UnusedParameter.Local
// ReSharper disable EmptyNamespace

using System;
using System.Data;
using System.Data.Common;

namespace Google.Cloud.Spanner
{
#if NET45 || NET451

    /// <summary>
    /// </summary>
    public sealed class SpannerDataAdapter : DbDataAdapter, IDbDataAdapter
    {
        /// <summary>
        /// </summary>
        public SpannerDataAdapter()
        {
        }

        /// <summary>
        /// </summary>
        /// <param name="sqlQuery"></param>
        /// <param name="connection"></param>
        public SpannerDataAdapter(string sqlQuery, SpannerConnection connection)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// </summary>
        public new SpannerCommand DeleteCommand
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        /// <summary>
        /// </summary>
        public new SpannerCommand InsertCommand
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        /// <summary>
        /// </summary>
        public new SpannerCommand SelectCommand
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        /// <summary>
        /// </summary>
        public new SpannerCommand UpdateCommand
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        IDbCommand IDbDataAdapter.DeleteCommand
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        IDbCommand IDbDataAdapter.InsertCommand
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        IDbCommand IDbDataAdapter.SelectCommand
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        IDbCommand IDbDataAdapter.UpdateCommand
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        /// <summary>
        /// </summary>
        public event EventHandler<SpannerRowUpdatedEventArgs> RowUpdated
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        /// <summary>
        /// </summary>
        public event EventHandler<SpannerRowUpdatingEventArgs> RowUpdating
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        /*
         * Implement abstract methods inherited from DbDataAdapter.
         */
        /// <inheritdoc />
        protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command,
            StatementType statementType, DataTableMapping tableMapping)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command,
            StatementType statementType, DataTableMapping tableMapping)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        protected override void OnRowUpdated(RowUpdatedEventArgs value)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        protected override void OnRowUpdating(RowUpdatingEventArgs value)
        {
            throw new NotImplementedException();
        }
    }

#endif
}