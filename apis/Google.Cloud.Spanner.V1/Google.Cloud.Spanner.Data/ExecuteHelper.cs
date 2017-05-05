using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.V1.Logging;

namespace Google.Cloud.Spanner
{
    internal static class ExecuteHelper
    {
        internal static void WithErrorTranslationAndProfiling(Action t, string name)
        {
            try
            {
                Logger.LogPerformanceCounterFn($"{name}.Count", x => x + 1);
                Stopwatch sw = null;
                if (Logger.LogPerformanceTraces)
                {
                    sw = Stopwatch.StartNew();
                }
                t();
                if (sw != null)
                {
                    Logger.LogPerformanceCounterFn($"{name}.Duration", x => sw.ElapsedMilliseconds);
                }
            }
            catch (Exception e)
            {
                SpannerException translatedException;
                if (SpannerException.TryTranslateRpcException(e, out translatedException))
                {
                    throw translatedException;
                }
                throw;
            }
        }

        internal static async Task WithErrorTranslationAndProfiling(Func<Task> t, string name)
        {
            try
            {
                Logger.LogPerformanceCounterFn($"{name}.Count", x => x + 1);
                Stopwatch sw = null;
                if (Logger.LogPerformanceTraces)
                {
                    sw = Stopwatch.StartNew();
                }
                await t();
                if (sw != null)
                {
                    Logger.LogPerformanceCounterFn($"{name}.Duration", x => sw.ElapsedMilliseconds);
                }
            }
            catch (Exception e)
            {
                SpannerException translatedException;
                if (SpannerException.TryTranslateRpcException(e, out translatedException))
                {
                    throw translatedException;
                }
                throw;
            }
        }

        internal static async Task<T> WithErrorTranslationAndProfiling<T>(Func<Task<T>> t, string name)
        {
            try
            {
                Logger.LogPerformanceCounterFn($"{name}.Count", x => x + 1);
                Stopwatch sw = null;
                if (Logger.LogPerformanceTraces)
                {
                    sw = Stopwatch.StartNew();
                }
                T result = await t();
                if (sw != null)
                {
                    Logger.LogPerformanceCounterFn($"{name}.Duration", x => sw.ElapsedMilliseconds);
                }
                return result;
            }
            catch (Exception e)
            {
                SpannerException translatedException;
                if (SpannerException.TryTranslateRpcException(e, out translatedException))
                {
                    throw translatedException;
                }
                throw;
            }
        }
    }
}