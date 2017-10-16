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
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Diagnostics
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// </summary>
    public class SpannerLogBridge<TLoggerCategory> : Cloud.Spanner.V1.Internal.Logging.DefaultLogger
        where TLoggerCategory : LoggerCategory<TLoggerCategory>, new()
    {
        private readonly IDiagnosticsLogger<TLoggerCategory> _efLogger;

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public SpannerLogBridge(IDiagnosticsLogger<TLoggerCategory> efLogger)
        {
            _efLogger = efLogger;
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override bool EnableSensitiveDataLogging
        {
            get => _efLogger.ShouldLogSensitiveData();
            set
            {
                //Not enabled here.  Use DbContextOptionsBuilder.EnableSensitiveDataLogging.
            }
        }
        
        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override void Debug(Func<string> messageFunc)
        {
            _efLogger.Logger.Log(Microsoft.Extensions.Logging.LogLevel.Debug,
                SpannerEventId.SpannerDiagnosticLog, messageFunc, null, (x, e) => x());
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override void Error(Func<string> messageFunc, Exception exception = null)
        {
            _efLogger.Logger.Log(Microsoft.Extensions.Logging.LogLevel.Error,
                SpannerEventId.SpannerDiagnosticLog, messageFunc, exception, (x, e) => $"{x()}, detail:{e}");
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override void Info(Func<string> messageFunc)
        {
            _efLogger.Logger.Log(Microsoft.Extensions.Logging.LogLevel.Information,
                SpannerEventId.SpannerDiagnosticLog, messageFunc, null, (x, e) => x());
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override void Warn(Func<string> messageFunc)
        {
            _efLogger.Logger.Log(Microsoft.Extensions.Logging.LogLevel.Warning,
                SpannerEventId.SpannerDiagnosticLog, messageFunc, null, (x, e) => x());
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override void LogPerformanceMessage(string message)
        {
            _efLogger.Logger.Log(Microsoft.Extensions.Logging.LogLevel.Information,
                SpannerEventId.SpannerDiagnosticLog, message, null, (x, e) => x);
        }
    }
}
