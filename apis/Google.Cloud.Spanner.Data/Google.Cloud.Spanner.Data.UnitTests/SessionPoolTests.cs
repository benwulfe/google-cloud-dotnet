using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.V1.Logging;
using Google.Protobuf;
using Grpc.Core;
using Moq;
using Xunit;
using Xunit.Abstractions;
// ReSharper disable AccessToDisposedClosure

namespace Google.Cloud.Spanner.Data.Tests
{
    public class SessionPoolTests
    {
        private static readonly DatabaseName s_defaultName =
            DatabaseName.Parse("projects/project1/instances/instance1/databases/database1");

        private ConcurrentDictionary<Mock<SpannerClient>, List<Session>> _createdSessions =
            new ConcurrentDictionary<Mock<SpannerClient>, List<Session>>();

        private ConcurrentStack<Transaction> _transactions = new ConcurrentStack<Transaction>();

        public SessionPoolTests(ITestOutputHelper outputHelper)
        {
            //Uncomment these lines to debug a specific test.
//            TestLogger.TestOutputHelper = outputHelper;
//            TestLogger.Install();
//            Logger.LogLevel = V1.Logging.LogLevel.Debug;
        }

        //Creates a client that will log created sessions in the _createdSession dictionary.
        private Mock<SpannerClient> CreateMockClient(DatabaseName dbName = null)
        {

            var mockClient = new Mock<SpannerClient>();
            if (dbName == null)
            {
                dbName = s_defaultName;
            }
            _createdSessions.TryAdd(mockClient, new List<Session>());

            mockClient.Setup(
                    x => x.CreateSessionAsync(
                        It.Is<DatabaseName>(y => y.Equals(dbName)),
                        It.IsAny<CancellationToken>()))
                .ReturnsAsync(
                    () =>
                    {
                        var mockSession = new Session
                        {
                            Name = $"{s_defaultName}/sessions/{Guid.NewGuid()}"
                        };
                        List<Session> sessionList;
                        if (_createdSessions.TryGetValue(mockClient, out sessionList))
                        {
                            lock (sessionList)
                            {
                                sessionList.Add(mockSession);
                            }
                        }
                        return mockSession;
                    });

            mockClient.Setup(
                    x => x.BeginTransactionAsync(It.IsAny<BeginTransactionRequest>(), It.IsAny<CallSettings>()))
                .ReturnsAsync(
                    () =>
                    {
                        var tx = new Transaction { Id = ByteString.CopyFromUtf8(Guid.NewGuid().ToString())};
                        _transactions.Push(tx);
                        return tx;
                    });

            mockClient.Setup(x => x.CommitAsync(It.IsAny<SessionName>(), It.IsAny<ByteString>(),
                It.IsAny<IEnumerable<Mutation>>(), It.IsAny<CallSettings>()))
                .ReturnsAsync(() => new CommitResponse());

            return mockClient;
        }

        [Fact]
        public void CanCreateSessionPool()
        {
            SessionPool pool = new SessionPool();
            pool.Dispose();
        }

        [Fact]

        public async Task EmptyPoolCreatesNewSession()
        {
            using (var pool = new SessionPool())
            {

                var client = CreateMockClient();
                var session = await pool.CreateSessionFromPoolAsync(
                        client.Object, s_defaultName.ProjectId,
                        s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);

                Assert.Equal(1, _createdSessions.Count);
                Assert.Equal(1, _createdSessions[client].Count);
                Assert.Same(session, _createdSessions[client][0]);
                client.Verify(
                    x => x.CreateSessionAsync(
                        It.Is<DatabaseName>(y => y.Equals(s_defaultName)),
                        It.IsAny<CancellationToken>()), Times.Once);
            }
        }

        [Fact]
        public async Task SynchronousRelease()
        {
            using (var pool = new SessionPool())
            {

                var client = CreateMockClient();
                var session = await pool.CreateSessionFromPoolAsync(
                        client.Object, s_defaultName.ProjectId,
                        s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);

                pool.ReleaseToPool(client.Object, session);
                var session2 = await pool.CreateSessionFromPoolAsync(
                        client.Object, s_defaultName.ProjectId,
                        s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);

                Assert.Same(session, session2);
            }
        }

        [Fact]
        public async Task ExpiredSessionsNotPooled()
        {
            using (var pool = new SessionPool())
            {

                var client = CreateMockClient();
                var session = await pool.CreateSessionFromPoolAsync(
                        client.Object, s_defaultName.ProjectId,
                        s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);

                SessionPool.MarkSessionExpired(session);
                pool.ReleaseToPool(client.Object, session);
                var session2 = await pool.CreateSessionFromPoolAsync(
                        client.Object, s_defaultName.ProjectId,
                        s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);

                Assert.NotSame(session, session2);
                Assert.Equal(1, _createdSessions.Count);
                Assert.Equal(2, _createdSessions[client].Count);
            }
        }

        [Fact]
        public async Task MaxPoolSizeNotViolated()
        {
            using (var pool = new SessionPool())
            {
                pool.Options.MaximumPooledSessions = 2;

                var client = CreateMockClient();
                var sessions = await Task.WhenAll(
                        pool.CreateSessionFromPoolAsync(
                            client.Object, s_defaultName.ProjectId,
                            s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None),
                        pool.CreateSessionFromPoolAsync(
                            client.Object, s_defaultName.ProjectId,
                            s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None),
                        pool.CreateSessionFromPoolAsync(
                            client.Object, s_defaultName.ProjectId,
                            s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None))
                    .ConfigureAwait(false);

                pool.ReleaseToPool(client.Object, sessions[0]);
                pool.ReleaseToPool(client.Object, sessions[1]);
                pool.ReleaseToPool(client.Object, sessions[2]);

                Assert.Equal(
                    2, pool.GetPoolSize(
                        client.Object, s_defaultName.ProjectId,
                        s_defaultName.InstanceId, s_defaultName.DatabaseId));
            }
        }

        [Fact]
        public async Task MaxActiveViolationThrows()
        {
            using (var pool = new SessionPool())
            {
                pool.Options.WaitOnResourcesExhausted = false;
                pool.Options.MaximumActiveSessions = 2;
                bool exceptionThrown = false;

                var client = CreateMockClient();
                await pool.CreateSessionFromPoolAsync(
                        client.Object, s_defaultName.ProjectId,
                        s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);
                await pool.CreateSessionFromPoolAsync(
                        client.Object, s_defaultName.ProjectId,
                        s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);
                try
                {
                    await pool.CreateSessionFromPoolAsync(
                        client.Object, s_defaultName.ProjectId,
                        s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (RpcException e)
                {
                    Assert.Equal(StatusCode.ResourceExhausted, e.Status.StatusCode);
                    exceptionThrown = true;
                }

                Assert.True(exceptionThrown);
            }
        }

        [Fact]
        public async Task MaxActiveViolationBlocks()
        {
            using (var pool = new SessionPool())
            {
                pool.Options.WaitOnResourcesExhausted = true;
                pool.Options.MaximumActiveSessions = 2;

                var client = CreateMockClient();
                var session1 = await pool.CreateSessionFromPoolAsync(
                    client.Object, s_defaultName.ProjectId,
                    s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);
                await pool.CreateSessionFromPoolAsync(
                    client.Object, s_defaultName.ProjectId,
                    s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);

                var releaseTask = Task.Delay(TimeSpan.FromMilliseconds(10)).ContinueWith(
                    (x, y) => { pool.ReleaseToPool(client.Object, session1); }, null);

                var createTask = pool.CreateSessionFromPoolAsync(
                    client.Object, s_defaultName.ProjectId,
                    s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None);

                await Task.WhenAll(createTask, releaseTask).ConfigureAwait(false);
                Assert.NotNull(createTask.ResultWithUnwrappedExceptions());
            }
        }

        [Fact]
        public async Task ClientsHaveDifferentPools()
        {
            using (var pool = new SessionPool())
            {

                var client1 = CreateMockClient();
                var client2 = CreateMockClient();

                var session = await pool.CreateSessionFromPoolAsync(
                    client1.Object, s_defaultName.ProjectId,
                    s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);
                pool.ReleaseToPool(client1.Object, session);

                var session2 = await pool.CreateSessionFromPoolAsync(
                    client2.Object, s_defaultName.ProjectId,
                    s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);

                Assert.NotSame(session, session2);
                Assert.Equal(2, _createdSessions.Count);
                Assert.Equal(1, _createdSessions[client1].Count);
                Assert.Equal(1, _createdSessions[client2].Count);
            }
        }

        [Fact]
        public async Task DatabasesHaveDifferentPools()
        {
            using (var pool = new SessionPool())
            {
                var client = CreateMockClient();

                var session = await pool.CreateSessionFromPoolAsync(
                    client.Object, s_defaultName.ProjectId,
                    s_defaultName.InstanceId, s_defaultName.DatabaseId, null, CancellationToken.None)
                    .ConfigureAwait(false);
                pool.ReleaseToPool(client.Object, session);

                var session2 = await pool.CreateSessionFromPoolAsync(
                    client.Object, "newproject", "newinstance", "newdbid", null, CancellationToken.None)
                    .ConfigureAwait(false);

                Assert.NotSame(session, session2);
                Assert.Equal(1, _createdSessions.Count);
                Assert.Equal(1, _createdSessions[client].Count);
            }
        }

        [Fact]
        public async Task SessionPreWarmTx()
        {
            using (var pool = new SessionPool())
            {
                var client = CreateMockClient();

                var txOptions = new TransactionOptions {ReadWrite = new TransactionOptions.Types.ReadWrite()};
                var session1 = await pool.CreateSessionFromPoolAsync(
                    client.Object, s_defaultName.ProjectId,
                    s_defaultName.InstanceId, s_defaultName.DatabaseId, txOptions, CancellationToken.None)
                    .ConfigureAwait(false);

                var transactionAwaited = await client.Object.BeginPooledTransactionAsync(session1, txOptions)
                    .ConfigureAwait(false);

                Transaction transaction;
                Assert.True(_transactions.TryPop(out transaction));
                Assert.Same(transaction, transactionAwaited);
                await transaction.CommitAsync(session1, null).ConfigureAwait(false);

                //Releasing should start the tx prewarm
                pool.ReleaseToPool(client.Object, session1);
                Transaction preWarmTx;
                var now = DateTime.Now;
                while (!_transactions.TryPop(out preWarmTx))
                {
                    await Task.Yield();
                    //everything is simulated, so the prewarm should be immediate.
                    Assert.True(DateTime.Now - now < TimeSpan.FromMilliseconds(500));
                }

                var session2 = await pool.CreateSessionFromPoolAsync(
                    client.Object, s_defaultName.ProjectId,
                    s_defaultName.InstanceId, s_defaultName.DatabaseId, txOptions, CancellationToken.None)
                    .ConfigureAwait(false);

                var transaction2 = await client.Object.BeginPooledTransactionAsync(session2, txOptions)
                    .ConfigureAwait(false);

                Assert.Same(preWarmTx, transaction2);
                Assert.True(_transactions.IsEmpty);
            }
        }

        private static Task<T>[] DuplicateTaskAsync<T>(Func<Task<T>> factory, int count)
        {
            List<Task<T>> taskList = new List<Task<T>>();
            for (; count > 0; count--)
            {
                taskList.Add(factory());
            }
            return taskList.ToArray();
        }

        private static Task[] DuplicateTaskAsync(Func<Task> factory, int count)
        {
            List<Task> taskList = new List<Task>();
            for (; count > 0; count--)
            {
                taskList.Add(factory());
            }
            return taskList.ToArray();
        }

        private async Task CreateReleaseWriteSessions(SessionPool pool, SpannerClient client, int iterations, int parallelCount)
        {
            await Task.Yield();
            for (var i = 0; i < iterations; i++)
            {
                var readWriteOptions = new TransactionOptions { ReadWrite = new TransactionOptions.Types.ReadWrite() };
                var writeSessions = await Task.WhenAll(
                    DuplicateTaskAsync(
                        () =>
                            pool.CreateSessionFromPoolAsync(
                                client, s_defaultName.ProjectId,
                                s_defaultName.InstanceId, s_defaultName.DatabaseId, readWriteOptions, CancellationToken.None), parallelCount))
                                .ConfigureAwait(false);

                await Task.Delay(TimeSpan.FromMilliseconds(10)).ConfigureAwait(false);

                foreach (var session in writeSessions)
                {
                    var transaction = await client.BeginPooledTransactionAsync(session, readWriteOptions)
                        .ConfigureAwait(false);
                    await transaction.CommitAsync(session, null).ConfigureAwait(false);
                    pool.ReleaseToPool(client, session);
                }
            }
        }

        private async Task CreateReleaseReadSessions(SessionPool pool, SpannerClient client, int iterations, int parallelCount)
        {
            await Task.Yield();
            for (var i = 0; i < iterations; i++)
            {
                var readOptions = new TransactionOptions { ReadOnly = new TransactionOptions.Types.ReadOnly() };
                var readSessions = await Task.WhenAll(
                    DuplicateTaskAsync(
                        () =>
                            pool.CreateSessionFromPoolAsync(
                                client, s_defaultName.ProjectId,
                                s_defaultName.InstanceId, s_defaultName.DatabaseId, readOptions, CancellationToken.None), parallelCount))
                                .ConfigureAwait(false);

                await Task.Delay(TimeSpan.FromMilliseconds(10)).ConfigureAwait(false);

                foreach (var session in readSessions)
                {
                    pool.ReleaseToPool(client, session);
                }
            }
        }

        [Fact]
        public async Task SessionStress()
        {
            using (var pool = new SessionPool())
            {
                var client = CreateMockClient();
                List<Task> stressTasks = new List<Task>();
                stressTasks.AddRange(DuplicateTaskAsync(() => CreateReleaseReadSessions(pool, client.Object, 25, 3), 5));
                stressTasks.AddRange(DuplicateTaskAsync(() => CreateReleaseWriteSessions(pool, client.Object, 25, 3), 5));
                await Task.WhenAll(stressTasks).ConfigureAwait(false);

                //There are 10 parallel stressors each creating no more than
                // 5 simultaneous sessions.  Therefore the maximum number of created
                // sessions should be no more than 50.
                Assert.Equal(1, _createdSessions.Count);
                Assert.True(_createdSessions[client].Count <= 30);
            }
        }
    }
}
