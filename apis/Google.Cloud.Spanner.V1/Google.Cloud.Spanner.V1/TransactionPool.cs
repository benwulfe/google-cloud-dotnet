using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1.Logging;
using Google.Protobuf;

namespace Google.Cloud.Spanner.V1
{
    /// <summary>
    /// The TransactionPool works in conjuntion with the Session pool to warm pooled Sessions
    /// with the transaction semantics it last used (if appropriate).
    /// When requesting a pooled session, the request can include transactionoptions which
    /// is then used as a key in the sessionpool.
    /// </summary>
    public static class TransactionPool
    {
        // Holds transaction state on sessions, including the last created transaction and options used.
        static readonly ConcurrentDictionary<Session, SessionInfo> s_sessionInfoTable = new ConcurrentDictionary<Session, SessionInfo>();

        // Holds a list of all active transactions with back pointers to the owning session.
        static readonly ConcurrentDictionary<ByteString, Session> s_activeTransactionTable = new ConcurrentDictionary<ByteString, Session>();

        /// <summary>
        /// Creates a transaction with the given transactoin options.
        /// Depending on the state of the session, this may or may not result in a request
        /// to Spanner over the network.
        /// </summary>
        /// <param name="session"></param>
        /// <param name="client"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public static async Task<Transaction> BeginPooledTransactionAsync(this SpannerClient client, Session session,
            TransactionOptions options)
        {
            var info = s_sessionInfoTable.GetOrAdd(session, s => new SessionInfo {SpannerClient = client});
        
            //we need to await for previous task completion anyway -- otherwise there is a bad race condition.
            await info.WaitForPreWarm().ConfigureAwait(false);
            
            //now we see if we can return the prewarmed transaction.
            // a lot has to be correct for us to return the precreated transaction.
            // we have to have a valid transaction object.
            // it needs to exist in the activetransactiontable to indicate that it hasn't been closed
            // the options on it must equal the passed in options.
            if (info.ActiveTransaction != null
                && !info.ActiveTransaction.Id.IsEmpty
                && s_activeTransactionTable.ContainsKey(info.ActiveTransaction.Id))
            {
                if (info.ActiveTransactionOptions != null
                    && info.ActiveTransactionOptions.Equals(options))
                {
                    return info.ActiveTransaction;
                }
                //cache hit, but no match on options
                Session ignored;
                s_activeTransactionTable.TryRemove(info.ActiveTransaction.Id, out ignored);
            }

            //ok, our cache hit didnt work for whatever reason.  Let's create a transaction with the given options and return it.
            await info.CreateTransactionAsync(session, options);
            if (info.ActiveTransaction != null)
            {
                s_activeTransactionTable.AddOrUpdate(info.ActiveTransaction.Id, session, (id, s) => session);
            }
            return info.ActiveTransaction;
        }

        /// <summary>
        /// Sets the transaction option on the session to be an implicit transaction.
        /// This option is often used when a session is used for reads without specifying a transaction.
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public static Task SetImplicitTransactionAsync(this Session session)
        {
            return RemoveFromTransactionPool(session);
        }

        /// <summary>
        /// When a session is deleted or no longer used, this will remove it from the transaction pool.
        /// </summary>
        /// <param name="session"></param>
        public static async Task RemoveFromTransactionPool(this Session session)
        {
            SessionInfo info;
            if (s_sessionInfoTable.TryGetValue(session, out info))
            {
                //we need to await for previous task completion anyway -- otherwise there is a bad race condition.
                await info.WaitForPreWarm().ConfigureAwait(false);
                if (info.ActiveTransaction != null && !info.ActiveTransaction.Id.IsEmpty)
                {
                    Session ignored;
                    s_activeTransactionTable.TryRemove(info.ActiveTransaction.Id, out ignored); //clear out old tx.
                }
                s_sessionInfoTable.TryRemove(session, out info); //implicit transaction is active on session.
            }
        }

        /// <summary>
        /// Returns the last used transactionoptions for CreateTransactionAsync.
        /// It may return null if the the session was set to use implicit transactions.
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public static TransactionOptions GetLastUsedTransactionOptions(this Session session)
        {
            SessionInfo info;
            if (s_sessionInfoTable.TryGetValue(session, out info))
            {
                return info.ActiveTransactionOptions;
            }
            return null;
        }


        /// <summary>
        /// Starts a pre-warm background task to create a transaction on the given session with the
        /// last used transaction options.  This method will only work if CreateTransactionAsync
        /// was previously used to create the transaction on the session.
        /// </summary>
        /// <param name="session"></param>
        /// <param name="client"></param>
        public static Task PreWarmTransactionAsync(this SpannerClient client, Session session)
        {
            TransactionOptions options = session.GetLastUsedTransactionOptions();
            var info = s_sessionInfoTable.GetOrAdd(session, s => new SessionInfo {SpannerClient = client});
            return info.PreWarmAsync(session, options);
        }

        /// <summary>
        /// Commits a transaction and internally marks the transaction as used.
        /// </summary>
        /// <param name="transaction"></param>
        /// <param name="session"></param>
        /// <param name="mutations"></param>
        /// <returns></returns>
        public static Task CommitAsync(this Transaction transaction, Session session, IEnumerable<Mutation> mutations)
        {
            return RunFinalMethodAsync(transaction, session,
                info => info.SpannerClient.CommitAsync(session.SessionName, info.ActiveTransaction.Id, mutations));
        }

        /// <summary>
        /// Rolls back a transaction and internally marks the transaction as used.
        /// </summary>
        /// <param name="transaction"></param>
        /// <param name="session"></param>
        /// <returns></returns>
        public static Task RollbackAsync(this Transaction transaction, Session session)
        {
            return RunFinalMethodAsync(transaction, session,
                info => info.SpannerClient.RollbackAsync(session.SessionName, info.ActiveTransaction.Id));
        }

        private static async Task RunFinalMethodAsync(Transaction transaction, Session session, Func<SessionInfo, Task> commitOrRollbackAction)
        {
            SessionInfo info;
            if (s_sessionInfoTable.TryGetValue(session, out info))
            {
                //we should have already waited, but extra checking cannot hurt.
                await info.WaitForPreWarm().ConfigureAwait(false);
                if (info.ActiveTransaction == null
                    || info.ActiveTransaction.Id.IsEmpty
                    || !Equals(info.ActiveTransaction.Id, transaction.Id))
                {
                    throw new InvalidOperationException("The transaction being committed was not found to have a valid entry.");
                }

                try
                {
                    await commitOrRollbackAction(info);
                }
                finally
                {
                    Session ignored;
                    s_activeTransactionTable.TryRemove(info.ActiveTransaction.Id, out ignored);
                }
            }
            else
            {
                throw new ArgumentException(
                    "The given transaction was not found in the transaction pool. Only "
                    +
                    "use this method on Transactions returned from TransactionPoool.CreateTransactionAsync.");
            }
        }

        private class SessionInfo
        {
            public Transaction ActiveTransaction { get; set; }
            public TransactionOptions ActiveTransactionOptions { get; set; }
            public SpannerClient SpannerClient { get; set; }
            private Task PreWarmTask { get; set; }

            public async Task WaitForPreWarm()
            {
                try
                {
                    Task prewarmTask = PreWarmTask;
                    if (prewarmTask != null && !prewarmTask.IsCompleted)
                    {
                        await prewarmTask.ConfigureAwait(false);
                    }
                    PreWarmTask = null;
                }
                catch (Exception e)
                {
                    Logger.Error(() => "An error occurred attemping to prewarm a session.", e);
                }
            }

            public async Task CreateTransactionAsync(Session session, TransactionOptions options)
            {
                await WaitForPreWarm().ConfigureAwait(false);  //this is a little redundant, but just to be sure.
                ActiveTransactionOptions = options;
                await CreateTransactionImplAsync(session).ConfigureAwait(false);
            }

            private async Task CreateTransactionImplAsync(Session session)
            {
                ActiveTransaction = await SpannerClient.BeginTransactionAsync(new BeginTransactionRequest
                {
                    SessionAsSessionName = session.SessionName,
                    Options = ActiveTransactionOptions
                }).ConfigureAwait(false);
                s_activeTransactionTable.AddOrUpdate(ActiveTransaction.Id, session, (id, s) => session);
            }

            public async Task PreWarmAsync(Session session, TransactionOptions options)
            {
                //we need to await for previous task completion anyway -- otherwise there is a bad race condition.
                //this is a little redundant, but just to be sure.
                await WaitForPreWarm().ConfigureAwait(false);
                // for now, we only prewarm readwrite transactions because the read transaction semantics are usually
                // dependent on the time the transaction begins.
                if (options!= null && options.ModeCase == TransactionOptions.ModeOneofCase.ReadWrite)
                {
                    Logger.Debug(() => $"Pre-warming session transaction state. Mode={options.ModeCase}");
                    ActiveTransactionOptions = options;
                    PreWarmTask = Task.Run(() => CreateTransactionImplAsync(session));
                }
            }
        }

    }
}
