﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1.Logging;

namespace Google.Cloud.Spanner.V1
{
    internal class SessionPoolImpl : IComparable
    {
        //This is the maximum we will search for a matching transaction option session.
        //We'll normally not hit this, but this is to stop abnormal cases where almost all
        //cached sessions are of one type, but another type is requested.
        private const int MaximumLinearSearchDepth = 50;

        private readonly List<SessionPoolEntry> _sessionMruStack = new List<SessionPoolEntry>();
        private int _lastAccessTime;
        private static int s_activeSessionsPooled;


        public static int ActiveSessionsPooled => s_activeSessionsPooled;

        public SessionPoolKey Key { get; }

        internal SessionPoolImpl(SessionPoolKey key)
        {
            Key = key;
        }


        internal Task ReleaseAllImpl()
        {
            SessionPoolEntry[] entries;
            lock (_sessionMruStack)
            {
                entries = _sessionMruStack.ToArray();
            }
            return Task.WhenAll(entries.Select(sessionpoolentry => EvictImmediately(sessionpoolentry.Session, CancellationToken.None)).ToArray());
        }

        private Task EvictSessionPoolEntry(Session session, CancellationToken cancellationToken)
        {
            var task = Task.Delay(SessionPool.PoolEvictTimeSpan, cancellationToken);
            return task.ContinueWith(async (delayTask, o) => 
            {
                await EvictImmediately(session, cancellationToken);
            }, null, cancellationToken);
        }

        private static void LogSessionsPooled()
        {
            Logger.LogPerformanceCounter("SessionsPooled", () => s_activeSessionsPooled);
        }

        private async Task EvictImmediately(Session session, CancellationToken cancellationToken)
        {
            SessionPoolEntry entry = default(SessionPoolEntry);
            lock (_sessionMruStack)
            {
                var found = _sessionMruStack.FindIndex(x => ReferenceEquals(x.Session, session));
                if (found != -1)
                {
                    entry = _sessionMruStack[found];
                    _sessionMruStack.RemoveAt(found);
                    Interlocked.Decrement(ref s_activeSessionsPooled);
                    LogSessionsPooled();
                }
            }
            if (entry.Session != null)
            {
                await Key.Client.DeleteSessionAsync(entry.Session.SessionName, cancellationToken).ConfigureAwait(false);
            }
        }

        private bool TryPop(TransactionOptions options, out SessionPoolEntry entry)
        {
            entry = new SessionPoolEntry();
            //we make a reasonable attempt at obtaining a session with the given transactionoptions.
            //but its not guaranteed.
            lock (_sessionMruStack)
            {
                if (_sessionMruStack.Count > 0)
                {
                    bool found = false;
                    for (int indexToUse = 0; indexToUse < _sessionMruStack.Count
                        && indexToUse < MaximumLinearSearchDepth; indexToUse++)
                    {
                        entry = _sessionMruStack[indexToUse];
                        if (Equals(entry.Session.GetLastUsedTransactionOptions(), options))
                        {
                            found = true;
                            _sessionMruStack.RemoveAt(indexToUse);
                            break;
                        }
                    }
                    if (!found)
                    {
                        entry = _sessionMruStack[0];
                        _sessionMruStack.RemoveAt(0);
                    }

                    Interlocked.Decrement(ref s_activeSessionsPooled);
                    LogSessionsPooled();
                    return true;
                }
            }
            entry = default(SessionPoolEntry);
            return false;
        }

        private void Push(SessionPoolEntry entry)
        {
            lock (_sessionMruStack)
            {
                _sessionMruStack.Insert(0, entry);
                Interlocked.Increment(ref s_activeSessionsPooled);
                LogSessionsPooled();
            }
        }

        public async Task<Session> AcquireSessionAsync(TransactionOptions options, CancellationToken cancellationToken)
        {
            SessionPoolEntry sessionEntry;
            if (!TryPop(options, out sessionEntry))
            {
                //create a new session, blocking or throwing if at the limit.
                return await Key.Client.CreateSessionAsync(new DatabaseName(Key.Project, Key.Instance, Key.Database), cancellationToken).ConfigureAwait(false);
            }
            MarkUsed();
            //note that the evict task will only actually delete the session if it was able to remove it from the pool.
            //at this point, this is not possible because we removed it from the pool, so even if the task completes (which
            // is possible due to a race), it will see that the session isn't pooled and cancel out.
            sessionEntry.EvictTaskCancellationSource.Cancel();
            return sessionEntry.Session;
        }

        public Session AcquireEvictionCandidate()
        {
            SessionPoolEntry sessionEntry = default(SessionPoolEntry);
            lock (_sessionMruStack)
            {
                if (_sessionMruStack.Count > 0)
                {
                    sessionEntry = _sessionMruStack[_sessionMruStack.Count - 1];
                    _sessionMruStack.RemoveAt(_sessionMruStack.Count - 1);
                    Interlocked.Decrement(ref s_activeSessionsPooled);
                    LogSessionsPooled();
                }
            }
            if (sessionEntry.Session == null || sessionEntry.EvictTaskCancellationSource == null)
            {
                return null;
            }
            sessionEntry.EvictTaskCancellationSource.Cancel();
            return sessionEntry.Session;
        }

        private void MarkUsed()
        {
            _lastAccessTime = Environment.TickCount;
        }

        public void ReleaseSessionToPool(SpannerClient client, Session session)
        {
            //start evict timer.
            SessionPoolEntry entry = new SessionPoolEntry {
                Session = session,
                EvictTaskCancellationSource = new CancellationTokenSource()
            };

            if (SessionPool.UseTransactionWarming)
            {
                Task.Run(() => client.PreWarmTransactionAsync(entry.Session));
            }
            Push(entry);
            //kick off the pool eviction timer.  This gets canceled when the item is pulled from the pool.
            Task.Run(() => EvictSessionPoolEntry(entry.Session, entry.EvictTaskCancellationSource.Token),
                entry.EvictTaskCancellationSource.Token);
        }

        public int CompareTo(object obj)
        {
            SessionPoolImpl other = obj as SessionPoolImpl;
            if (other == null)
            {
                return -1;
            }
            if (other._sessionMruStack.Count > _sessionMruStack.Count)
            {
                return 1;
            }
            if (other._sessionMruStack.Count < _sessionMruStack.Count)
            {
                return -1;
            }
            return _lastAccessTime.CompareTo(other._lastAccessTime);
        }
    }
}
