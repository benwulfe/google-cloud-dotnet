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
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Util;
using Google.Cloud.Spanner.V1;

#if NET451
using Transaction = System.Transactions.Transaction;

#endif

// ReSharper disable UnusedParameter.Local
namespace Google.Cloud.Spanner
{
    /// <summary>
    ///     A SpannerConnection represents a connection to a single Spanner database.
    /// </summary>
    public sealed class SpannerConnection : DbConnection
    {
        //Internally, a SpannerConnection acts as a local SessionPool.
        //When OpenAsync() is called, it creates a session with passthru transaction semantics and
        //allows other consumers to borrow that session.
        //Consumers may be SpannerTransactions or if the user has is not explicitly using transactions,
        //the consumer may be the SpannerCommand itself.
        //While SpannerTransaction has sole ownership of the session it obtains, SpannerCommand shares
        // any session it obtains with others.

        private static readonly TransactionOptions s_defaultTransactionOptions = new TransactionOptions();

        private SpannerBatchOperation _activeBatchOperation;
        private SpannerClient _client;
        private SpannerConnectionStringBuilder _connectionStringBuilder;
        private Session _sharedSession;
        private int _sessionRefCount;
        private ConnectionState _state = ConnectionState.Closed;
        private readonly object _sync = new object();

        /// <summary>
        ///     Creates a SpannerConnection with no datasource or credential specified.
        /// </summary>
        public SpannerConnection()
        {
        }

        /// <summary>
        ///     Creates a SpannerConnection with a datasource contained in connectionString
        ///     and optional credential information supplied in connectionString or the credential
        ///     argument.
        /// </summary>
        /// <param name="connectionString">A Spanner formatted connection string.</param>
        /// <param name="credential">An optional credential.</param>
        public SpannerConnection(string connectionString, ITokenAccess credential = null)
            : this(new SpannerConnectionStringBuilder(connectionString, credential))
        {
        }

        /// <summary>
        ///     Creates a SpannerConnection with a datasource contained in connectionString.
        /// </summary>
        /// <param name="connectionStringBuilder">
        ///     A SpannerConnectionStringBuilder containing
        ///     a formatted connection string.
        /// </param>
        public SpannerConnection(SpannerConnectionStringBuilder connectionStringBuilder)
        {
            connectionStringBuilder.ThrowIfNull(nameof(connectionStringBuilder));
            TrySetNewConnectionInfo(connectionStringBuilder);
        }

        /// <summary>
        ///     Provides options to customize how connections to Spanner are created
        ///     and maintained.
        /// </summary>
        public static ConnectionPoolOptions ConnectionPoolOptions => ConnectionPoolOptions.Instance;

        /// <inheritdoc />
        public override string ConnectionString
        {
            get { return _connectionStringBuilder.ToString(); }
            set
            {
                TrySetNewConnectionInfo(new SpannerConnectionStringBuilder(value, _connectionStringBuilder.Credential));
            }
        }

        /// <summary>
        ///     Returns the credential used for the connection, if set.
        /// </summary>
        public ITokenAccess Credential => _connectionStringBuilder.Credential;

        /// <inheritdoc />
        public override string Database => _connectionStringBuilder.SpannerDatabase;

        /// <inheritdoc />
        public override string DataSource => _connectionStringBuilder.DataSource;

        /// <summary>
        /// </summary>
        public string Project => _connectionStringBuilder.Project;

        //TODO review server version support.
        /// <inheritdoc />
        public override string ServerVersion => "0.0";

        /// <summary>
        /// </summary>
        public string SpannerInstance => _connectionStringBuilder.SpannerInstance;

        /// <inheritdoc />
        public override ConnectionState State => _state;


        internal bool IsClosed => (State & ConnectionState.Open) == 0;

        internal bool IsOpen => (State & ConnectionState.Open) == ConnectionState.Open;

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public SpannerBatchOperation BeginBatchOperation()
        {
            //This may be called while the connection is closed.
            var newBatchOperation = new SpannerBatchOperation(this);
            if (Interlocked.CompareExchange(ref _activeBatchOperation, newBatchOperation, null)
                != null) throw new InvalidOperationException("You may only create a single Batch operation at a time.");
            return _activeBatchOperation;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<SpannerTransaction> BeginReadOnlyTransactionAsync(
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return BeginReadOnlyTransactionAsync(new TransactionOptions.Types.ReadOnly { Strong = true}, cancellationToken);
        }

        private async Task<SpannerTransaction> BeginTransactionImpl(CancellationToken cancellationToken, TransactionOptions transactionOptions, TransactionMode transactionMode)
        {
            using (var sessionHolder = await SessionHolder.Allocate(this, transactionOptions, cancellationToken).ConfigureAwait(false))
            {
                var transaction = await SpannerClient.BeginPooledTransactionAsync(sessionHolder.Session, transactionOptions).ConfigureAwait(false);
                return new SpannerTransaction(this, transactionMode, sessionHolder.TakeOwnership(),
                    transaction);
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="readonlyType"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<SpannerTransaction> BeginReadOnlyTransactionAsync(TransactionOptions.Types.ReadOnly readonlyType, 
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return BeginTransactionImpl(cancellationToken, new TransactionOptions {
                ReadOnly = readonlyType
            }, TransactionMode.ReadOnly);
        }

        /// <summary>
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<SpannerTransaction> BeginTransactionAsync(
            CancellationToken cancellationToken = default(CancellationToken))
        {

            return BeginTransactionImpl(cancellationToken, new TransactionOptions {
                ReadWrite = new TransactionOptions.Types.ReadWrite()
            }, TransactionMode.ReadWrite);
        }

        /// <inheritdoc />
        public override void ChangeDatabase(string newDataSource)
        {
            TrySetNewConnectionInfo(_connectionStringBuilder.CloneWithNewDataSource(newDataSource));
        }

        /// <inheritdoc />
        public override void Close()
        {
            Session session;
            bool primarySessionInUse;
            lock (_sync)
            {
                if (IsClosed) return;
                primarySessionInUse = _sessionRefCount > 0;
                _state = ConnectionState.Closed;
                //we do not await the actual session close, we let that happen async.
                session = _sharedSession;
                _sharedSession = null;
            }
            if (session != null && !primarySessionInUse)
            {
                //release the session if its not used.
                //if it's used, we'll detect a closed state after the operation and release it then.
                var task = SpannerClient.ReleaseToPool(session);
                task.ContinueWith(t =>
                {
                    //TODO, proper logging.
                    //if (t.IsFaulted && t.Exception != null) Trace.TraceWarning($"Error releasing session: {t.Exception}");
                });
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="databaseTable"></param>
        /// <param name="deleteFilterParameters"></param>
        /// <returns></returns>
        public SpannerCommand CreateDeleteCommand(string databaseTable,
            SpannerParameterCollection deleteFilterParameters = null)
        {
            return new SpannerCommand(SpannerCommandTextBuilder.CreateDeleteTextBuilder(databaseTable), this, null,
                deleteFilterParameters);
        }

        /// <summary>
        /// </summary>
        /// <param name="databaseTable"></param>
        /// <param name="insertParameters"></param>
        /// <returns></returns>
        public SpannerCommand CreateInsertCommand(string databaseTable,
            SpannerParameterCollection insertParameters = null)
        {
            return new SpannerCommand(SpannerCommandTextBuilder.CreateInsertTextBuilder(databaseTable), this, null,
                insertParameters);
        }

        /// <summary>
        /// </summary>
        /// <param name="databaseTable"></param>
        /// <param name="insertUpdateParameters"></param>
        /// <returns></returns>
        public SpannerCommand CreateInsertOrUpdateCommand(string databaseTable,
            SpannerParameterCollection insertUpdateParameters = null)
        {
            return new SpannerCommand(SpannerCommandTextBuilder.CreateInsertOrUpdateTextBuilder(databaseTable), this,
                null, insertUpdateParameters);
        }

        /// <summary>
        /// </summary>
        /// <param name="sqlQueryStatement"></param>
        /// <param name="selectParameters"></param>
        /// <returns></returns>
        public SpannerCommand CreateSelectCommand(string sqlQueryStatement,
            SpannerParameterCollection selectParameters = null)
        {
            return new SpannerCommand(SpannerCommandTextBuilder.CreateSelectTextBuilder(sqlQueryStatement), this, null,
                selectParameters);
        }

        /// <summary>
        /// </summary>
        /// <param name="databaseTable"></param>
        /// <param name="updateParameters"></param>
        /// <returns></returns>
        public SpannerCommand CreateUpdateCommand(string databaseTable,
            SpannerParameterCollection updateParameters = null)
        {
            return new SpannerCommand(SpannerCommandTextBuilder.CreateUpdateTextBuilder(databaseTable), this, null,
                updateParameters);
        }

        /// <inheritdoc />
        public override void Open()
        {
            if (IsOpen)
                return;
            OpenAsync(CancellationToken.None).Wait();
        }

        /// <inheritdoc />
        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            if (IsOpen) return;
            lock (_sync)
            {
                if (IsOpen) return;
                if (_state == ConnectionState.Connecting)
                    throw new InvalidOperationException("The SpannerConnection is already being opened.");
                _state = ConnectionState.Connecting;
            }
            try
            {
                _client = await ClientPool.AcquireClientAsync(_connectionStringBuilder.Credential,
                    _connectionStringBuilder.EndPoint).ConfigureAwait(false);
                _sharedSession = await SpannerClient.CreateSessionFromPoolAsync(
                    _connectionStringBuilder.Project,
                    _connectionStringBuilder.SpannerInstance,
                    _connectionStringBuilder.SpannerDatabase,
                    s_defaultTransactionOptions,
                    cancellationToken).ConfigureAwait(false);
                _sessionRefCount = 0;
            }
            finally
            {
                _state = _sharedSession != null ? ConnectionState.Open : ConnectionState.Broken;
            }
        }

        /// <inheritdoc />
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            isolationLevel.Precondition(nameof(isolationLevel), IsolationLevel.Serializable, IsolationLevel.Unspecified);
            return BeginTransactionAsync().Result;
        }

        /// <inheritdoc />
        protected override DbCommand CreateDbCommand()
        {
            return new SpannerCommand();
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (IsOpen) Close();
        }

        private void AssertClosed(string message)
        {
            if (!IsClosed)
                throw new InvalidOperationException("The connection must be closed.  Failed to " + message);
        }

        private void AssertOpen(string message)
        {
            if (!IsOpen)
                throw new InvalidOperationException("The connection must be open.  Failed to " + message);
        }

        private void TrySetNewConnectionInfo(SpannerConnectionStringBuilder newBuilder)
        {
            AssertClosed("change connection information.");
            _connectionStringBuilder = newBuilder;
        }


        internal async Task ReleaseSession(Session session)
        {
            if (ReferenceEquals(session, _sharedSession))
            {
                Interlocked.Decrement(ref _sessionRefCount);
            }
            else
            {
                await SpannerClient.ReleaseToPool(session).ConfigureAwait(false);
            }
        }

        private volatile Task<Session> _sharedSessionAllocator;

        private Task<Session> AllocateSession(TransactionOptions options, CancellationToken cancellationToken)
        {
            AssertOpen("execute command");
            Task<Session> result;

            lock (_sync)
            {
                //we will use _sharedSession if
                //a) options == s_defaultTransactionOptions && _sharedSession != null
                //    (we increment refcnt) and return _sharedSession
                //b) options == s_defaultTransactionOptions && _sharedSession == null
                //    we create a new shared session and return it.
                //c) options != s_defaultTransactionOptions && _sharedSession != null && refcnt == 0
                //    we steal _sharedSession to return it, set _sharedSession = null
                //d) options != s_defaultTransactionOptions && (_sharedSession == null || refcnt > 0)
                //    we grab a new session from the pool.
                bool isSharedReadonlyTx = Equals(options, s_defaultTransactionOptions);
                if (isSharedReadonlyTx && _sharedSession != null)
                {
                    var taskSource = new TaskCompletionSource<Session>();
                    taskSource.SetResult(_sharedSession);
                    result = taskSource.Task;
                    Interlocked.Increment(ref _sessionRefCount);
                }
                else if (isSharedReadonlyTx && _sharedSession == null)
                {
                    // If we enter this code path, it means a transaction has stolen our shared session.
                    // This is ok, we'll just create another.  But need to be very careful about concurrency
                    // as compared to OpenAsync (which is documented as not threadsafe).
                    // To make this threadsafe, we store the creation task as a member and let other callers
                    // hook onto the first creation task.
                    if (_sharedSessionAllocator == null)
                    {
                        _sharedSessionAllocator = SpannerClient.CreateSessionFromPoolAsync(
                            _connectionStringBuilder.Project, _connectionStringBuilder.SpannerInstance,
                            _connectionStringBuilder.SpannerDatabase, options, CancellationToken.None);
                        _sharedSessionAllocator.ContinueWith(t =>
                        {
                            if (t.IsCompleted)
                            {
                                // we need to make sure the refcnt is >0 before setting _sharedSession.
                                // or else the session could again be stolen from us by another transaction.
                                Interlocked.Increment(ref _sessionRefCount);
                                _sharedSession = t.Result;
                            }
                        }, CancellationToken.None);
                    }
                    else
                    {
                        _sharedSessionAllocator.ContinueWith(t =>
                        {
                            if (t.IsCompleted)
                            {
                                Interlocked.Increment(ref _sessionRefCount);
                            }
                        }, CancellationToken.None);
                    }
                    result = _sharedSessionAllocator;
                }
                else if (!isSharedReadonlyTx && _sharedSession != null && _sessionRefCount == 0)
                {
                    //In this case, someone has called OpenAsync() followed by BeginTransactionAsync().
                    //While we'd prefer them to just call BeginTransaction (so we can allocate a session
                    // with the appropriate transaction semantics straight from the pool), this is still allowed
                    // and we shouldn't create *two* sessions here for the case where they only ever use
                    // this connection for a single transaction.
                    //So, we'll steal the shared precreated session and re-allocate it to the transaction.
                    //If the user later does reads outside of a transaction, it will force create a new session.
                    var taskSource = new TaskCompletionSource<Session>();
                    taskSource.SetResult(_sharedSession);
                    result = taskSource.Task;
                    _sessionRefCount = 0;
                    _sharedSession = null;
                    _sharedSessionAllocator = null;
                }
                else
                {
                    //In this case, its a transaction and the shared session is also in use.
                    //so, we'll just create a new session (from the pool).
                    result = SpannerClient.CreateSessionFromPoolAsync(
                        _connectionStringBuilder.Project, _connectionStringBuilder.SpannerInstance,
                        _connectionStringBuilder.SpannerDatabase, options, cancellationToken);
                }

            }

            return result;
        }

        /// <summary>
        /// SessionHolder is a helper class to ensure that sessions do not leak and are properly recycled when
        /// an error occurs.
        /// </summary>
        internal sealed class SessionHolder : IDisposable
        {
            private readonly SpannerConnection _connection;
            private Session _session;

            private SessionHolder(SpannerConnection connection, Session session)
            {
                _connection = connection;
                _session = session;
            }

            public static Task<SessionHolder> Allocate(SpannerConnection owner,  CancellationToken cancellationToken)
            {
                return Allocate(owner, s_defaultTransactionOptions, cancellationToken);
            }

            public static async Task<SessionHolder> Allocate(SpannerConnection owner, TransactionOptions options, CancellationToken cancellationToken)
            {
                var session = await owner.AllocateSession(options, cancellationToken).ConfigureAwait(false);
                return new SessionHolder(owner, session);
            }

            public Session Session => _session;

            public Session TakeOwnership()
            {
                return Interlocked.Exchange(ref _session, null);
            }
            public void Dispose()
            {
                var session = Interlocked.Exchange(ref _session, null);
                if (session != null)
                {
                    Task.Run(() => _connection.ReleaseSession(session));
                }
            }
        }
        internal SpannerClient SpannerClient => _client;

#if NET451

        /// <inheritdoc />
        public override void EnlistTransaction(Transaction transaction)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        protected override DbProviderFactory DbProviderFactory => SpannerProviderFactory.s_instance;

#endif
    }
}