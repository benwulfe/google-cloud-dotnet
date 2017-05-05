using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner
{
    /// <summary>
    /// 
    /// </summary>
    public static class TransientFaultDetector
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static bool IsTransientSpannerFault(this Exception exception)
        {
            SpannerException spannerException;
            SpannerException.TryTranslateRpcException(exception, out spannerException);

            return spannerException != null && spannerException.IsRetryable;
        }
    }
}