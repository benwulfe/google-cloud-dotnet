﻿// Copyright 2017 Google Inc. All Rights Reserved.
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

        private SpannerClientFactory()
        {
        }

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
