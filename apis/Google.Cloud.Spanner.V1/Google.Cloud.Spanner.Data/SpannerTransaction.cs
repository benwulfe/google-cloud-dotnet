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
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.V1.Logging;

// ReSharper disable UnusedParameter.Local

namespace Google.Cloud.Spanner
{
    /// <summary>
    /// </summary>
    public sealed class SpannerTransaction : DbTransaction, ISpannerTransaction
    {
        private static long s_transactionCount;
        private readonly SpannerConnection _connection;
        private readonly List<Mutation> _mutations = new List<Mutation>();
        private readonly Transaction _transaction;

        internal SpannerTransaction(SpannerConnection connection, TransactionMode mode, Session session,
            Transaction transaction)
        {
            connection.AssertNotNull(nameof(connection));
            session.AssertNotNull(nameof(session));
            transaction.AssertNotNull(nameof(transaction));

            Logger.LogPerformanceCounter("Transactions.ActiveCount",
                () => Interlocked.Increment(ref s_transactionCount));

            Session = session;
            _transaction = transaction;
            _connection = connection;
            Mode = mode;
        }

        /// <inheritdoc />
        public override IsolationLevel IsolationLevel => IsolationLevel.Serializable;

        /// <summary>
        /// </summary>
        public TransactionMode Mode { get; }

        /// <summary>
        /// </summary>
        public Session Session { get; }

        /// <inheritdoc />
        protected override DbConnection DbConnection => _connection;

        internal IEnumerable<Mutation> Mutations => _mutations;

        Task<int> ISpannerTransaction.ExecuteMutationsAsync(List<Mutation> mutations,
            CancellationToken cancellationToken)
        {
            mutations.AssertNotNull(nameof(mutations));
            CheckCompatibleMode(TransactionMode.ReadWrite);
            return ExecuteHelper.WithErrorTranslationAndProfiling(() =>
            {
                var taskCompletionSource = new TaskCompletionSource<int>();
                cancellationToken.ThrowIfCancellationRequested();
                _mutations.AddRange(mutations);
                taskCompletionSource.SetResult(mutations.Count);
                return taskCompletionSource.Task;
            }, "SpannerTransaction.ExecuteMutations");
        }

        Task<ReliableStreamReader> ISpannerTransaction.ExecuteQueryAsync(string sql,
            CancellationToken cancellationToken)
        {
            sql.AssertNotNullOrEmpty(nameof(sql));
            return ExecuteHelper.WithErrorTranslationAndProfiling(() =>
            {
                var taskCompletionSource =
                    new TaskCompletionSource<ReliableStreamReader>();
                taskCompletionSource.SetResult(_connection.SpannerClient.GetSqlStreamReader(new ExecuteSqlRequest {
                    Sql = sql,
                    Transaction = GetTransactionSelector(TransactionMode.ReadOnly)
                }, Session));

                return taskCompletionSource.Task;
            }, "SpannerTransaction.ExecuteQuery");
        }

        /// <inheritdoc />
        public override void Commit()
        {
            if (!Task.Run(CommitAsync).Wait(ConnectionPoolOptions.Instance.TimeoutMilliseconds))
                throw new SpannerException(ErrorCode.DeadlineExceeded, "The Commit did not complete in time.");
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public Task CommitAsync()
        {
            return ExecuteHelper.WithErrorTranslationAndProfiling(() =>
            {
                Mode.AssertTrue(x => x != TransactionMode.ReadOnly, "You cannot commit a readonly transaction.");
                return _transaction.CommitAsync(Session, Mutations);
            }, "SpannerTransaction.Commit");
        }

        /// <inheritdoc />
        public override void Rollback()
        {
            if (!Task.Run(RollbackAsync).Wait(ConnectionPoolOptions.Instance.TimeoutMilliseconds))
                throw new SpannerException(ErrorCode.DeadlineExceeded, "The Rollback did not complete in time.");
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public Task RollbackAsync()
        {
            return ExecuteHelper.WithErrorTranslationAndProfiling(() =>
            {
                Mode.AssertTrue(x => x == TransactionMode.ReadOnly, "You cannot roll back a readonly transaction.");
                return _transaction.RollbackAsync(Session);
            }, "SpannerTransaction.Rollback");
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            Logger.LogPerformanceCounter("Transactions.ActiveCount",
                () => Interlocked.Decrement(ref s_transactionCount));
            _connection.ReleaseSession(Session);
        }

        private void CheckCompatibleMode(TransactionMode mode)
        {
            switch (mode)
            {
                case TransactionMode.ReadOnly:
                    Mode.AssertTrue(x => x == TransactionMode.ReadOnly || x == TransactionMode.ReadWrite,
                        "You can only execute reads on a ReadWrite or ReadOnly Transaction!");
                    break;
                case TransactionMode.ReadWrite:
                    Mode.AssertTrue(x => x == TransactionMode.ReadWrite,
                        "You can only execute read/write commands on a ReadWrite Transaction!");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }
        }

        private TransactionSelector GetTransactionSelector(TransactionMode mode)
        {
            CheckCompatibleMode(mode);
            _transaction.AssertTrue(x => x != null, "Transaction should have been created prior to use.");
            return new TransactionSelector {Id = _transaction?.Id};
        }
    }
}