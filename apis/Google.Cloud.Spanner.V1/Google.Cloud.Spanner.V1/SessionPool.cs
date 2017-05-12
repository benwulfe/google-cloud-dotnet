﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Util;
using Google.Cloud.Spanner.V1.Logging;
using Grpc.Core;

namespace Google.Cloud.Spanner.V1
{
    /// <summary>
    /// The SessionPool is the preferred method of managing Spanner Sessions.
    /// It performs pooling of sessions with an evict timer along with configurable settings to control the maximum
    /// number of pooled and active sessions.
    /// To create a session using the sessionpool, use the extension method on SpannerClient.
    ///   var session = await spannerClient.CreateSessionFromPoolAsync(project, spannerInstance, database, cancellationtoken);
    /// To release a session back to the sessionpool:
    ///   session.ReleaseToPool();
    /// If you use the SessionPool, you must make sure sessions are properly released back into the pool and are not simply GC'd.
    /// Allowing a session to GC will incur penalties on the current process as the session will count towards the maximum allowed
    /// and it will incur a penalty on other spanner processes because it will be an hour before the server frees the session if 
    /// its not properly deleted.
    /// </summary>
    public static class SessionPool
    {
        private static readonly TimeSpan s_shutDownTimer = TimeSpan.FromSeconds(60);

        // This member holds information we'll use when the session later gets released.
        private static readonly ConcurrentDictionary<Session, SessionPoolKey>
            s_sessionsInUse = new ConcurrentDictionary<Session, SessionPoolKey>();

        private static int s_sessionsCreating;
        private static readonly ConcurrentQueue<TaskCompletionSource<int>> s_waitQueue = new ConcurrentQueue<TaskCompletionSource<int>>();
        private static readonly object s_waitSync = new object();

        private static ConcurrentDictionary<SessionPoolKey, SessionPoolImpl> GlobalPool { get; } =
            new ConcurrentDictionary<SessionPoolKey, SessionPoolImpl>();
        private static readonly object s_priorityListSync = new object();
        private static SortedList<SessionPoolImpl, SessionPoolImpl> PriorityList { get; }
            = new SortedList<SessionPoolImpl, SessionPoolImpl>();

        private static readonly ConcurrentDictionary<string, bool> s_blackListedSessions = new ConcurrentDictionary<string, bool>();

        static SessionPool()
        {
#if NET45
            AppDomain.CurrentDomain.DomainUnload += (sender, args) =>
            {
                Task.Run(ReleaseAll).Wait(s_shutDownTimer);
            };
#endif
#if NETSTANDARD1_5
            System.Runtime.Loader.AssemblyLoadContext.Default.Unloading += context =>
            {
                Task.Run(ReleaseAll).Wait(s_shutDownTimer);
            };
#endif
        }

        private static Task WhenCanceled(this CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        private static async Task StartSessionCreating(CancellationToken cancellationToken)
        {
            lock (s_waitSync)
            {
                if (CurrentActiveSessions < MaximumActiveSessions)
                {
                    s_sessionsCreating++;
                    return;
                }
            }
            var canceledTask = cancellationToken.WhenCanceled();
            //we need to block or throw because we are out of allocations.
            while (true)
            {
                if (WaitOnResourcesExhausted)
                {
                    TaskCompletionSource<int> signal = new TaskCompletionSource<int>();
                    s_waitQueue.Enqueue(signal);
                    await Task.WhenAny(signal.Task, canceledTask).ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();
                    lock (s_waitSync)
                    {
                        if (CurrentActiveSessions < MaximumActiveSessions)
                        {
                            s_sessionsCreating++;
                            return;
                        }
                    }
                }
                else
                {
                    throw new RpcException(new Status(StatusCode.ResourceExhausted, "Number of available Sessions exhausted."));
                }
            }
        }

        private static void EndSessionCreating()
        {
            lock (s_waitSync)
            {
                s_sessionsCreating--;
            }
            SignalAnyWaitingRequests();
        }

        /// <summary>
        /// Marks a session as expired so that it will never be pooled in the sessionpool
        /// </summary>
        /// <param name="session"></param>
        public static void MarkSessionExpired(Session session)
        {
            if (session != null)
            {
                s_blackListedSessions.TryAdd(session.Name, true);
            }
        }

        /// <summary>
        /// Returns true if the given session has been marked as expired.
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public static bool IsSessionExpired(Session session)
        {
            bool unused;
            return session != null && s_blackListedSessions.TryGetValue(session.Name, out unused);
        }

        /// <summary>
        /// Allocates a session from the pool if possible, or creates a new Spanner Session.
        /// </summary>
        /// <param name="spannerClient"></param>
        /// <param name="project"></param>
        /// <param name="spannerInstance"></param>
        /// <param name="spannerDatabase"></param>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<Session> CreateSessionFromPoolAsync(this SpannerClient spannerClient,
            string project, string spannerInstance, string spannerDatabase, TransactionOptions options, CancellationToken cancellationToken)
        {
            project.ThrowIfNullOrEmpty(nameof(project));
            spannerInstance.ThrowIfNullOrEmpty(nameof(spannerInstance));
            spannerDatabase.ThrowIfNullOrEmpty(nameof(spannerDatabase));
            
            await StartSessionCreating(cancellationToken).ConfigureAwait(false);
            try
            {
                var sessionPoolKey = new SessionPoolKey {
                    Client = spannerClient,
                    Project = project,
                    Instance = spannerInstance,
                    Database = spannerDatabase
                };
                SessionPoolImpl targetPool = GlobalPool.GetOrAdd(sessionPoolKey, key => new SessionPoolImpl(key));

                var session = await targetPool.AcquireSessionAsync(options, cancellationToken);
                if (session != null)
                {
                    //refresh the mru list which tells us what sessions need to be trimmed from the pool when we want
                    // to add another poolEntry.
                    ReSortMru(targetPool);
                    s_sessionsInUse.AddOrUpdate(session, x => sessionPoolKey, (x, y) => sessionPoolKey);
                    LogSessionsInUse();
                }
                return session;
            }
            finally
            {
                EndSessionCreating();
            }
        }

        private static void SignalAnyWaitingRequests()
        {
            TaskCompletionSource<int> waitingSessionRequest;
            //if anyone is waiting, let them query for their session.
            if (CurrentActiveSessions < MaximumActiveSessions
                && s_waitQueue.TryDequeue(out waitingSessionRequest))
            {
                waitingSessionRequest.SetResult(0);
            }
        }

        private static void LogSessionsInUse()
        {
            Logger.LogPerformanceCounter("Session.ActiveCount", () => s_sessionsInUse.Count);
        }

        /// <summary>
        /// Releases a session back into the pool, possibly causing another entry to be evicted if the maximum pool size has been
        /// reached.
        /// </summary>
        /// <param name="session"></param>
        /// <param name="client"></param>
        /// <returns></returns>
        public static void ReleaseToPool(this SpannerClient client, Session session )
        {
            session.ThrowIfNull(nameof(session));

            SessionPoolKey result;
            if (s_sessionsInUse.TryRemove(session, out result))
            {
                LogSessionsInUse();
                SignalAnyWaitingRequests();
                if (IsSessionExpired(session))
                {
                    bool unused;
                    s_blackListedSessions.TryRemove(session.Name, out unused);
                    return;
                }
                SessionPoolImpl targetPool = GlobalPool.GetOrAdd(result,
                                             key => new SessionPoolImpl(key));

                SpannerClient evictionClient = result.Client;
                Session evictionSession = null;

                //Figure out if we want to pool this released session.
                lock (s_priorityListSync)
                {
                    targetPool.ReleaseSessionToPool(client, session);
                    if (CurrentPooledSessions > MaximumPooledSessions)
                    {
                        var evictionPool = PriorityList.First().Value;
                        evictionClient = evictionPool.Key.Client;
                        evictionSession = evictionPool.AcquireEvictionCandidate();
                        ReSortMru(evictionPool);
                    }
                    ReSortMru(targetPool);
                }
                if (evictionSession != null)
                {
                    Task.Run(() => EvictSession(evictionClient, evictionSession));
                }
            }
        }

        private static async Task EvictSession(SpannerClient evictionClient, Session evictionSession)
        {
            await evictionSession.RemoveFromTransactionPool().ConfigureAwait(false);
            await evictionClient.DeleteSessionAsync(evictionSession.GetSessionName());
        }

        /// <summary>
        /// Called to close a session and release it without putting it back into the pool.
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public static async Task Close(this Session session)
        {
            session.ThrowIfNull(nameof(session));

            SessionPoolKey result;
            if (s_sessionsInUse.TryRemove(session, out result))
            {
                LogSessionsInUse();
                SignalAnyWaitingRequests();
                await result.Client.DeleteSessionAsync(session.GetSessionName()).ConfigureAwait(false);
                return;
            }
            throw new InvalidOperationException("Close Session was not able to locate the provided session.");
        }

        private static void ReSortMru(SessionPoolImpl targetPool)
        {
            lock (s_priorityListSync)
            {
                if (PriorityList.Count > 1)
                {
                    PriorityList.Remove(targetPool);
                    PriorityList.Add(targetPool, targetPool);
                }
            }
        }

        /// <summary>
        /// Releases all pooled sessions and frees resources on the server.
        /// </summary>
        /// <returns></returns>
        public static Task ReleaseAll()
        {
            var entries = GlobalPool.Values;
            GlobalPool.Clear(); // ReleaseAll should not be called while other operations are starting.
            return Task.WhenAll(entries.Select(sessionpool => sessionpool.ReleaseAllImpl()).ToArray());
        }

        /// <summary>
        /// The current number of active sessions in use by the application.
        /// </summary>
        public static int CurrentActiveSessions => s_sessionsInUse.Count + s_sessionsCreating;

        /// <summary>
        /// The current number of sessions in the Session Pool.
        /// </summary>
        public static int CurrentPooledSessions => SessionPoolImpl.ActiveSessionsPooled;

        /// <summary>
        /// Maximum number of active sessions that can be in use by the application at any one time.
        /// </summary>
        public static int MaximumActiveSessions { get; set; } = int.MaxValue;

        /// <summary>
        /// The maximum number of sessions that can be held in the session pool.
        /// </summary>
        public static int MaximumPooledSessions { get; set; } = int.MaxValue;

        /// <summary>
        /// If true, then CreateSessionFromPoolAsync will block until CurrentActiveSessions is less than MaximumActiveSessions
        /// If false, then CreateSessionFromPoolAsync will throw an exception if CurrentActiveSessions is equal to or greater than MaximumActiveSessions
        /// </summary>
        public static bool WaitOnResourcesExhausted { get; set; } = true;

        /// <summary>
        /// The amount of time before sessions get forcibly evicted from the session pool.
        /// A lower value will cause the process to free sessions more aggressively when they get released
        /// at the cost of performance due to lower reuse of sessions.
        /// This value must be less than the expire timer on the Spanner server currently set at 60 minutes.
        /// </summary>
        public static TimeSpan PoolEvictionDelay { get; set; } = TimeSpan.FromMinutes(15);


        /// <summary>
        /// If set to true, Sessions placed back into the pool will have a new transaction created in the background with
        /// the same settings that were just used.  This will allow a later consumer of this session to skip creating a new
        /// transaction if the options were identical.
        /// </summary>
        public static bool UseTransactionWarming { get; set; } = true;

        /// <summary>
        /// 
        /// </summary>
        public static TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);
    }
}
