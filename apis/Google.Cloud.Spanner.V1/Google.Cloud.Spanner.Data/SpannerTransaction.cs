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
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;

// ReSharper disable UnusedParameter.Local

namespace Google.Cloud.Spanner
{
    /// <summary>
    /// </summary>
    public sealed class SpannerTransaction : DbTransaction, ISpannerTransaction
    {
        private readonly SpannerConnection _connection;
        private readonly Transaction _transaction;
        private readonly List<Mutation> _mutations = new List<Mutation>();

        internal SpannerTransaction(SpannerConnection connection, TransactionMode mode, Session session, Transaction transaction) 
        {
            Session = session;
            _transaction = transaction;
            _connection = connection;
            Mode = mode;
        }

        /// <inheritdoc />
        public override IsolationLevel IsolationLevel => IsolationLevel.Serializable;

        /// <inheritdoc />
        protected override DbConnection DbConnection => _connection;

        /// <summary>
        /// 
        /// </summary>
        public TransactionMode Mode { get; }

        /// <summary>
        /// 
        /// </summary>
        public Session Session { get; }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            _connection.ReleaseSession(Session).Start();
        }

        /// <inheritdoc />
        public override void Commit()
        {
            CommitAsync().Wait();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task CommitAsync()
        {
            Mode.Precondition(x => x != TransactionMode.ReadOnly, "You cannot commit a readonly transaction.");
            await _transaction.CommitAsync(Session, Mutations);
        }

        internal IEnumerable<Mutation> Mutations => _mutations;

        /// <inheritdoc />
        public override void Rollback()
        {
            RollbackAsync().Wait();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task RollbackAsync()
        {
            Mode.Precondition(x => x == TransactionMode.ReadOnly, "You cannot roll back a readonly transaction.");
            await _transaction.RollbackAsync(Session);
        }

        private void CheckCompatibleMode(TransactionMode mode)
        {
            switch (mode)
            {
                case TransactionMode.ReadOnly:
                    Mode.Precondition(x => x == TransactionMode.ReadOnly || x == TransactionMode.ReadWrite,
                        "You can only execute reads on a ReadWrite or ReadOnly Transaction!");
                    break;
                case TransactionMode.ReadWrite:
                    Mode.Precondition(x => x == TransactionMode.ReadWrite,
                        "You can only execute read/write commands on a ReadWrite Transaction!");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }
        }

        private TransactionSelector GetTransactionSelector(TransactionMode mode)
        {
            CheckCompatibleMode(mode);
            _transaction.Precondition(x => x != null, "Transaction should have been created prior to use.");
            return new TransactionSelector { Id = _transaction?.Id };
        }

        Task<int> ISpannerTransaction.ExecuteMutationsAsync(List<Mutation> mutations, CancellationToken cancellationToken)
        {
            CheckCompatibleMode(TransactionMode.ReadWrite);

            TaskCompletionSource<int> taskCompletionSource = new TaskCompletionSource<int>();
            cancellationToken.ThrowIfCancellationRequested();
            _mutations.AddRange(mutations);
            taskCompletionSource.SetResult(mutations.Count);
            return taskCompletionSource.Task;
        }

        Task<ReliableStreamReader> ISpannerTransaction.ExecuteQueryAsync(string sql, CancellationToken cancellationToken)
        {
            TaskCompletionSource<ReliableStreamReader> taskCompletionSource = new TaskCompletionSource<ReliableStreamReader>();
            taskCompletionSource.SetResult(_connection.SpannerClient.GetSqlStreamReader(new ExecuteSqlRequest {
                Sql = sql,
                Transaction = GetTransactionSelector(TransactionMode.ReadOnly)
            }, Session));

            return taskCompletionSource.Task;
        }
    }
}