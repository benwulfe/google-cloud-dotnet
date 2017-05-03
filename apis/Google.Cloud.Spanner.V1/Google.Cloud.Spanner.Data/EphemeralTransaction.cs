using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;

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
            int count;
            using (var transaction =  await _connection.BeginTransactionAsync(cancellationToken))
            {
                count = await ((ISpannerTransaction) transaction).ExecuteMutationsAsync(mutations, cancellationToken);
                await transaction.CommitAsync();
            }
            return count;
        }

        public async Task<ReliableStreamReader> ExecuteQueryAsync(string sql, CancellationToken cancellationToken)
        {
            using (SpannerConnection.SessionHolder holder = await SpannerConnection.SessionHolder.Allocate(_connection, cancellationToken))
            {
                var streamReader = _connection.SpannerClient.GetSqlStreamReader(new ExecuteSqlRequest
                {
                    Sql = sql,
                }, holder.TakeOwnership());

                streamReader.StreamClosed += (o, e) =>
                {
                    _connection.ReleaseSession(streamReader.Session).Start();
                };

                return streamReader;
            }
        }


    }
}
