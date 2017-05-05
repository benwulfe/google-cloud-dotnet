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
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.Spanner
{
    /// <summary>
    /// </summary>
    public sealed class ConnectionPoolOptions
    {
        private ConnectionPoolOptions()
        {
        }

        /// <summary>
        ///     The default instance of connection pool options.
        /// </summary>
        public static ConnectionPoolOptions Instance { get; } = new ConnectionPoolOptions();

        /// <summary>
        /// </summary>
        public TimeSpan PoolEvictTimeSpan
        {
            get { return SessionPool.PoolEvictTimeSpan; }
            set { SessionPool.PoolEvictTimeSpan = value; }
        }

        /// <summary>
        /// </summary>
        public int MaximumPooledSessions
        {
            get { return SessionPool.MaximumPooledSessions; }
            set { SessionPool.MaximumPooledSessions = value; }
        }

        /// <summary>
        /// </summary>
        public int MaximumActiveSessions
        {
            get { return SessionPool.MaximumActiveSessions; }
            set { SessionPool.MaximumActiveSessions = value; }
        }

        /// <summary>
        /// </summary>
        public ResourcesExhaustedBehavior ResourcesExhaustedBehavior
        {
            get
            {
                return SessionPool.WaitOnResourcesExhausted
                    ? ResourcesExhaustedBehavior.Block
                    : ResourcesExhaustedBehavior.Fail;
            }
            set { SessionPool.WaitOnResourcesExhausted = value == ResourcesExhaustedBehavior.Block; }
        }

        /// <summary>
        /// Sets the log level for diagnostic logs sent to Trace (for desktop) or stderr (for .Net Core).
        /// </summary>
        public LogLevel LogLevel
        {
            get
            {
                switch (V1.Logging.Logger.LogLevel)
                {
                    case V1.Logging.LogLevel.Debug:
                        return LogLevel.Debug;
                    case V1.Logging.LogLevel.Info:
                        return LogLevel.Info;
                    case V1.Logging.LogLevel.Warn:
                        return LogLevel.Warn;
                    case V1.Logging.LogLevel.Error:
                        return LogLevel.Error;
                    case V1.Logging.LogLevel.None:
                        return LogLevel.None;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            set
            {
                switch (value)
                {
                    case LogLevel.Debug:
                        V1.Logging.Logger.LogLevel = V1.Logging.LogLevel.Debug;
                        break;
                    case LogLevel.Info:
                        V1.Logging.Logger.LogLevel = V1.Logging.LogLevel.Info;
                        break;
                    case LogLevel.Warn:
                        V1.Logging.Logger.LogLevel = V1.Logging.LogLevel.Warn;
                        break;
                    case LogLevel.Error:
                        V1.Logging.Logger.LogLevel = V1.Logging.LogLevel.Error;
                        break;
                    case LogLevel.None:
                        V1.Logging.Logger.LogLevel = V1.Logging.LogLevel.None;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(value), value, null);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool LogPerformanceTraces
        {
            get { return V1.Logging.Logger.LogPerformanceTraces; }
            set { V1.Logging.Logger.LogPerformanceTraces = value; }
        }

        /// <summary>
        /// 
        /// </summary>
        public int PerformanceTraceLogInterval
        {
            get { return V1.Logging.Logger.PerformanceTraceLogInterval; }
            set { V1.Logging.Logger.PerformanceTraceLogInterval = value; }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool ResetPerformanceTracesEachInterval
        {
            get { return V1.Logging.Logger.ResetPerformanceTracesEachInterval; }
            set { V1.Logging.Logger.ResetPerformanceTracesEachInterval = value; }
        }

        /// <summary>
        /// </summary>
        public int TimeoutMilliseconds { get; set; } = 600000;

        /// <summary>
        /// 
        /// </summary>
        public TimeSpan KeepAliveIntervalMinutes { get; set; } = TimeSpan.FromMinutes(55);
    }
}