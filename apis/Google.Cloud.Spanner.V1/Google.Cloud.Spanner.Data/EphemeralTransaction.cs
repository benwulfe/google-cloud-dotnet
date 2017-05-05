﻿using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.V1.Logging;

namespace Google.Cloud.Spanner
{
    /// <summary>
    /// EphemeralTransaction acquires sessions from the provided SpannerConnection on an as-needed basis.
    /// It holds no long term refcounts or state of its own and can be created and GC'd at will.
    /// </summary>
    internal sealed class EphemeralTransaction : ISpannerTransaction
    {
        private readonly SpannerConnection _connection;

        public EphemeralTransaction(SpannerConnection connection)
        {
            connection.AssertNotNull(nameof(connection));
            _connection = connection;
        }

        /// <summary>
        /// Acquires a read/write transaction from Spannerconnection and releases the transaction back into the pool
        /// after the operation is complete.
        /// SpannerCommand never uses implicit Transactions for write operations because they are non idempotent and
        /// may result in unexpected errors if retry is used.
        /// </summary>
        /// <param name="mutations"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<int> ExecuteMutationsAsync(List<Mutation> mutations, CancellationToken cancellationToken)
        {
            mutations.AssertNotNull(nameof(mutations));
            Logger.Debug(() => "Executing a mutation change through an ephemeral transaction.");
            int count;

            using (var transaction = await _connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                count = await ((ISpannerTransaction) transaction).ExecuteMutationsAsync(mutations, cancellationToken)
                    .ConfigureAwait(false);
                await transaction.CommitAsync().ConfigureAwait(false);
            }
            return count;
        }

        public Task<ReliableStreamReader> ExecuteQueryAsync(string sql, CancellationToken cancellationToken)
        {
            return ExecuteHelper.WithErrorTranslationAndProfiling(async () =>
            {
                sql.AssertNotNullOrEmpty(nameof(sql));
                Logger.Debug(() => "Executing a query through an ephemeral transaction.");

                using (SpannerConnection.SessionHolder holder = await SpannerConnection.SessionHolder
                    .Allocate(_connection, cancellationToken)
                    .ConfigureAwait(false))
                {
                    var streamReader = _connection.SpannerClient.GetSqlStreamReader(new ExecuteSqlRequest {
                        Sql = sql,
                    }, holder.TakeOwnership());

                    streamReader.StreamClosed += (o, e) => { _connection.ReleaseSession(streamReader.Session); };

                    return streamReader;
                }
            }, "Transaction.ExecuteQuery");
        }
    }
}