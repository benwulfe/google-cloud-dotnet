﻿using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

// ReSharper disable PrivateFieldCanBeConvertedToLocalVariable
// ReSharper disable NotAccessedField.Local
// ReSharper disable EmptyDestructor

namespace Google.Cloud.Spanner.V1
{
    /// <summary>
    /// Provides streaming access to a Spanner SQL query that automatically retries, handles
    /// chunking and recoverable errors.
    /// </summary>
    public sealed class ReliableStreamReader : IDisposable
    {
        //WIP: add actual stream reads, retries, etc.
        private AsyncServerStreamingCall<PartialResultSet> _currentCall;
        private readonly object _sync = new object();
        private readonly SpannerClient _spannerClient;
        private readonly ExecuteSqlRequest _request;
        private readonly Session _session;
        private readonly CallSettings _callTiming;
        private ResultSetMetadata _metadata;
        private bool _isReading = true;

        private ByteString _resumeToken;
        private int _resumeSkipCount;

        internal ReliableStreamReader(SpannerClient spannerClientImpl, ExecuteSqlRequest request, Session session)
        {
            _spannerClient = spannerClientImpl;
            _request = request;
            _session = session;
            _clock = SpannerSettings.GetDefault().Clock ?? SystemClock.Instance;
            _scheduler = SpannerSettings.GetDefault().Scheduler ?? (IScheduler) SystemScheduler.Instance;

            _callTiming = CallSettings.FromCallTiming(CallTiming.FromRetry(new RetrySettings(
                    retryBackoff: SpannerSettings.GetDefaultRetryBackoff(),
                    timeoutBackoff: SpannerSettings.GetDefaultTimeoutBackoff(),
                    totalExpiration: Expiration.FromTimeout(TimeSpan.FromMilliseconds(600000)),
                    retryFilter: SpannerSettings.IdempotentRetryFilter
                )));
            _request.SessionAsSessionName = _session.SessionName;
        }

        private async Task<Metadata> ConnectAsync()
        {
            if (_resumeToken != null)
            {
                _request.ResumeToken = _resumeToken;
            }
            _currentCall = _spannerClient.ExecuteSqlStream(_request);
            return await _currentCall.ResponseHeadersAsync;
        }

        /// <summary>
        /// Ensures we have executed the query initially or from recovery and have read the initial metadata
        /// </summary>
        /// <returns></returns>
        private async Task<bool> ReliableConnect(CancellationToken cancellationToken)
        {
            if (_currentCall == null)
            {
                Func<Task<Metadata>> connectFn = ConnectAsync;
                var responseHeaders = await connectFn.CallWithRetry(_callTiming, _clock, _scheduler);

                Debug.Assert(_currentCall != null, "_currentCall != null");
                cancellationToken.ThrowIfCancellationRequested();

                for (int i = 0; i <= _resumeSkipCount; i++)
                {
                    _isReading = await _currentCall.ResponseStream.MoveNext(cancellationToken);
                    if (!_isReading)
                    {
                        return false;
                    }
                }
                if (_currentCall.ResponseStream.Current == null)
                {
                    throw new InvalidOperationException("stream read error!");
                }
                RecordResumeToken();
                if (_metadata == null)
                {
                    _metadata = _currentCall.ResponseStream.Current.Metadata;
                }
            }
            return _isReading;
        }

        private async Task<bool> ReliableMoveNext(CancellationToken cancellationToken)
        {
            try
            {
                _resumeSkipCount++;
                await _currentCall.ResponseStream.MoveNext(cancellationToken);
            }
            catch (Exception e)  //todo idempotent filter.
            {
                //reconnect on failure which will call reliableconnect and respect resumetoken and resumeskip
                cancellationToken.ThrowIfCancellationRequested();
                _currentCall = null;
                return await ReliableConnect(cancellationToken);
            }
            RecordResumeToken();
            return _isReading;
        }

        private void RecordResumeToken()
        {
            if (_isReading && _currentCall != null)
            {
                //record resume information.
                if (_currentCall.ResponseStream.Current.ResumeToken != null)
                {
                    _resumeToken = _currentCall.ResponseStream.Current.ResumeToken;
                    _resumeSkipCount = 0;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<ResultSetMetadata> GetMetadataAsync(CancellationToken cancellationToken)
        {
            await ReliableConnect(cancellationToken);
            return _metadata;
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsClosed { get; private set; }

        /// <summary>
        /// 
        /// 
        /// </summary>
        public void Close()
        {
            lock (_sync)
            {
                if (IsClosed)
                {
                    return;
                }
                IsClosed = true;
            }
            StreamClosed?.Invoke(this, new StreamClosedEventArgs());
        }

        /// <summary>
        /// 
        /// </summary>
        public event EventHandler<StreamClosedEventArgs> StreamClosed;

        void IDisposable.Dispose()
        {
            Close();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> HasData(CancellationToken cancellationToken)
        {
            return await ReliableConnect(cancellationToken);
        }


        private int _currentIndex;
        private readonly IClock _clock;
        private readonly IScheduler _scheduler;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<Value> Next(CancellationToken cancellationToken)
        {
            if (!await HasData(cancellationToken))
            {
                return null;
            }
            if (_currentIndex >= _currentCall.ResponseStream.Current.Values.Count)
            {
                //we need to move next
                _isReading = await ReliableMoveNext(cancellationToken);
                _currentIndex = 0;
                if (!_isReading)
                {
                    return null;
                }
            }
            var result = _currentCall.ResponseStream.Current.Values[_currentIndex];
            _currentIndex++;
            return result;
        }

        /// <inheritdoc />
        ~ReliableStreamReader()
        {
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class StreamClosedEventArgs : EventArgs
    {
    }
}
