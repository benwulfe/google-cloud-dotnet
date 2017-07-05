using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.V1.Logging;
using Grpc.Auth;
using Grpc.Core;

namespace Google.Cloud.Spanner.Data
{
    internal interface ISpannerClientFactory
    {
        Task<SpannerClient> CreateClientAsync(ServiceEndpoint endpoint, ITokenAccess credential);
    }

    /// <summary>
    /// For mockability and testability of the <see cref="ClientPool"/> class.
    /// </summary>
    internal class SpannerClientFactory : ISpannerClientFactory
    {
        internal static readonly ISpannerClientFactory Default = new SpannerClientFactory();

        private static async Task<ChannelCredentials> CreateDefaultChannelCredentialsAsync()
        {
            var appDefaultCredentials = await GoogleCredential.GetApplicationDefaultAsync().ConfigureAwait(false);
            if (appDefaultCredentials.IsCreateScopedRequired)
            {
                appDefaultCredentials = appDefaultCredentials.CreateScoped(SpannerClient.DefaultScopes);
            }
            return appDefaultCredentials.ToChannelCredentials();
        }

        /// <inheritdoc />
        public async Task<SpannerClient> CreateClientAsync(ServiceEndpoint endpoint, ITokenAccess credential)
        {
            ChannelCredentials channelCredentials;

            if (credential == null)
            {
                channelCredentials = await CreateDefaultChannelCredentialsAsync().ConfigureAwait(false);
            }
            else
            {
                channelCredentials = credential.ToChannelCredentials();
            }

            var channel = new Channel(
                endpoint.Host,
                endpoint.Port,
                channelCredentials);
            Logger.LogPerformanceCounterFn("SpannerClient.RawCreateCount", x => x + 1);

            //Pull the timeout from spanner options.
            //The option must be set before OpenAsync is called.
            var callSettings = CallSettings.FromCallTiming(
                CallTiming.FromRetry(
                    new RetrySettings(
                        SpannerSettings.GetDefaultRetryBackoff(),
                        SpannerSettings.GetDefaultTimeoutBackoff(),
                        Expiration.FromTimeout(SpannerOptions.Instance.Timeout),
                        SpannerSettings.NonIdempotentRetryFilter
                    )));

            return SpannerClient.Create(
                channel, new SpannerSettings
                {
                    CreateSessionSettings = callSettings,
                    GetSessionSettings = callSettings,
                    DeleteSessionSettings = callSettings,
                    ExecuteSqlSettings = callSettings,
                    ReadSettings = callSettings,
                    BeginTransactionSettings = callSettings,
                    CommitSettings = callSettings,
                    RollbackSettings = callSettings
                });
        }
    }
}
