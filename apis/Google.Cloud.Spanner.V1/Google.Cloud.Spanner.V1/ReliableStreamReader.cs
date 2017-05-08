using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.Spanner.V1.Logging;
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
            _scheduler = SpannerSettings.GetDefault().Scheduler ?? SystemScheduler.Instance;

            _callTiming = CallSettings.FromCallTiming(CallTiming.FromRetry(new RetrySettings(
                    SpannerSettings.GetDefaultRetryBackoff(),
                    SpannerSettings.GetDefaultTimeoutBackoff(),
                    Expiration.FromTimeout(TimeSpan.FromMilliseconds(600000)),
                    SpannerSettings.IdempotentRetryFilter
                )));
            _request.SessionAsSessionName = _session.GetSessionName();
        }

        private async Task<Metadata> ConnectAsync()
        {
            Logger.LogPerformanceCounterFn("StreamReader.ConnectCount", x => x + 1);
            if (_resumeToken != null)
            {
                Logger.Debug(() => $"Resuming at location:{_resumeToken}");
                _request.ResumeToken = _resumeToken;
            }
            _currentCall = _spannerClient.ExecuteSqlStream(_request);
            return await _currentCall.ResponseHeadersAsync.ConfigureAwait(false);
        }

        /// <summary>
        /// Connects or reconnects to Spanner, fast forwarding to where we left off based on
        /// our _resumeToken and _resumeSkipCount.
        /// </summary>
        /// <returns></returns>
        private async Task<bool> ReliableConnect(CancellationToken cancellationToken)
        {
            if (_currentCall == null)
            {
//                Func<Task<Metadata>> connectFn = ConnectAsync;
//                var responseHeaders = await connectFn.CallWithRetry(_callTiming, _clock, _scheduler);
                await ConnectAsync().ConfigureAwait(false);

                Debug.Assert(_currentCall != null, "_currentCall != null");
                cancellationToken.ThrowIfCancellationRequested();

                for (int i = 0; i <= _resumeSkipCount; i++)
                {
                    //This calls a simple movenext on purpose.  If we get an error here, we'll fail out.
                    _isReading = await _currentCall.ResponseStream.MoveNext(cancellationToken);
                    if (!_isReading || _currentCall.ResponseStream.Current == null)
                    {
                        return false;
                    }
                    if (_metadata == null)
                    {
                        _metadata = _currentCall.ResponseStream.Current.Metadata;
                    }
                }
                RecordResumeToken();
            }
            return _isReading;
        }

        private async Task<bool> ReliableMoveNext(CancellationToken cancellationToken)
        {
            try
            {
                Logger.LogPerformanceCounterFn("StreamReader.MoveNextCount", x => x + 1);
                //we increment our skip count before calling MoveNext so that a reconnect operation
                //will fast forward to the proper place.
                _resumeSkipCount++;
                _isReading = await _currentCall.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) 
            {
                _currentCall = null;

                //reconnect on failure which will call reliableconnect and respect resumetoken and resumeskip
                cancellationToken.ThrowIfCancellationRequested();

                Logger.Warn(() => $"An error occurred attemping to iterate through the sql query.  Attempting to recover. Exception:{e}");

                //when we reconnect, we purposely do not do a *reliable*movenext.  If we fail to fast forward on the reconnect
                //we bail out completely and surface the error.
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
            await ReliableConnect(cancellationToken).ConfigureAwait(false);
            return _metadata;
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsClosed { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        public Session Session => _session;

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
            Value result = await NextChunk(cancellationToken).ConfigureAwait(false);
            while (result != null && _currentCall.ResponseStream.Current.ChunkedValue &&
                   _currentIndex >= _currentCall.ResponseStream.Current.Values.Count)
            {
                result.ChunkedMerge(await NextChunk(cancellationToken));
            }
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<Value> NextChunk(CancellationToken cancellationToken)
        {
            if (!await HasData(cancellationToken).ConfigureAwait(false))
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
    public sealed class StreamClosedEventArgs : EventArgs
    {
    }
}
