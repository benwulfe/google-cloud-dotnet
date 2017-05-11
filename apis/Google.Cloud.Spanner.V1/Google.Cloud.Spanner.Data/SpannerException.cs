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
using System.Linq;
using Google.Cloud.Spanner.V1.Logging;
using Grpc.Core;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.Spanner
{
    /// <summary>
    /// Represents an error communicating with the Spanner database.
    /// </summary>
    public sealed class SpannerException : Exception
    {
        /// <summary>
        /// This class is a thin conversion around a grpc exception, with the additional
        /// information of whether the operation is retryable based on the resulting error.
        /// </summary>
        /// <param name="code"></param>
        /// <param name="innerException"></param>
        internal SpannerException(ErrorCode code, RpcException innerException)
            : base(GetMessageFromErrorCode(code), innerException)
        {
            Logger.LogPerformanceCounterFn("SpannerException.Count", x => x + 1);
            ErrorCode = innerException.IsSessionExpiredError() ? ErrorCode.Aborted : code;
        }

        internal SpannerException(ErrorCode code, string message)
            : base(message)
        {
            Logger.LogPerformanceCounterFn("SpannerException.Count", x => x + 1);
            ErrorCode = code;
        }

        internal SpannerException(RpcException innerException)
            : this(ConvertFromStatusCode(innerException.Status.StatusCode), innerException)
        {
        }

        internal static bool TryTranslateRpcException(Exception possibleRpcException,
            out SpannerException spannerException)
        {
            spannerException = possibleRpcException as SpannerException;

            RpcException rpcException = null;
            AggregateException aggregateException = possibleRpcException as AggregateException;

            if (aggregateException?.InnerExceptions != null)
            {
                spannerException = (SpannerException) aggregateException.InnerExceptions
                    .FirstOrDefault(x => x is SpannerException);
                rpcException = (RpcException) aggregateException.InnerExceptions
                    .FirstOrDefault(x => x is RpcException);
            }
            else
            {
                rpcException = possibleRpcException as RpcException;
            }

            if (rpcException != null)
            {
                spannerException = new SpannerException(rpcException);
            }

            return spannerException != null;
        }

        private static ErrorCode ConvertFromStatusCode(StatusCode statusCode)
        {
            switch (statusCode)
            {
                case StatusCode.OK:
                case StatusCode.Unknown:
                    return ErrorCode.Unknown;
                case StatusCode.Cancelled:
                    return ErrorCode.Cancelled;
                case StatusCode.InvalidArgument:
                    return ErrorCode.InvalidArgument;
                case StatusCode.DeadlineExceeded:
                    return ErrorCode.DeadlineExceeded;
                case StatusCode.NotFound:
                    return ErrorCode.NotFound;
                case StatusCode.AlreadyExists:
                    return ErrorCode.AlreadyExists;
                case StatusCode.PermissionDenied:
                    return ErrorCode.PermissionDenied;
                case StatusCode.Unauthenticated:
                    return ErrorCode.Unauthenticated;
                case StatusCode.ResourceExhausted:
                    return ErrorCode.ResourceExhausted;
                case StatusCode.FailedPrecondition:
                    return ErrorCode.FailedPrecondition;
                case StatusCode.Aborted:
                    return ErrorCode.Aborted;
                case StatusCode.OutOfRange:
                    return ErrorCode.OutOfRange;
                case StatusCode.Unimplemented:
                    return ErrorCode.Unimplemented;
                case StatusCode.Internal:
                    return ErrorCode.Internal;
                case StatusCode.Unavailable:
                    return ErrorCode.Unavailable;
                case StatusCode.DataLoss:
                    return ErrorCode.DataLoss;
                default:
                    throw new ArgumentOutOfRangeException(nameof(statusCode), statusCode, null);
            }
        }

        /// <summary>
        /// </summary>
        public ErrorCode ErrorCode { get; }

        /// <summary>
        /// </summary>
        public bool IsRetryable
        {
            get
            {
                switch (ErrorCode)
                {
                    case ErrorCode.DeadlineExceeded:
                    case ErrorCode.Aborted:
                    case ErrorCode.Unavailable:
                        return true;
                    default:
                        return false;
                }
            }
        }

        static string GetMessageFromErrorCode(ErrorCode errorCode)
        {
            switch (errorCode)
            {
                case ErrorCode.Cancelled:
                    return "The operation was canceled.";
                case ErrorCode.InvalidArgument:
                    return "An invalid argument was sent to Spanner.";
                case ErrorCode.DeadlineExceeded:
                    return "The operation deadline was exceeded.";
                case ErrorCode.NotFound:
                    return "Object not found.";
                case ErrorCode.AlreadyExists:
                    return "Object already exists.";
                case ErrorCode.PermissionDenied:
                    return "Insufficient permission to execute the specified operation.";
                case ErrorCode.Unauthenticated:
                    return "Invalid authentication credentials for the operation.";
                case ErrorCode.ResourceExhausted:
                    return "Resources have been exhausted.";
                case ErrorCode.FailedPrecondition:
                    return
                        "Operation was rejected because the system is not in a state required for the operation's execution.";
                case ErrorCode.Aborted:
                    return "The operation was aborted.";
                case ErrorCode.OutOfRange:
                    return "Operation was attempted past the valid range.";
                case ErrorCode.Unimplemented:
                    return "Operation is not implemented or not supported/enabled in this service.";
                case ErrorCode.Internal:
                    return "Internal error.";
                case ErrorCode.Unavailable:
                    return "The service is currently unavailable.  This is a most likely a transient condition.";
                case ErrorCode.DataLoss:
                    return "Unrecoverable data loss or corruption.";
                case ErrorCode.Unknown:
                    return "An unknown error occurred.";
                default:
                    throw new ArgumentOutOfRangeException(nameof(errorCode), errorCode, null);
            }
        }
    }
}