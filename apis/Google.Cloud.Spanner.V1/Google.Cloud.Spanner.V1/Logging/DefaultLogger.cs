using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.V1.Logging
{
    internal class DefaultLogger : Logger
    {
        private const int PerformanceLogInterval = 30000;
        private int _perfLoggingTaskEnabled;
        private readonly ConcurrentDictionary<string, PerformanceTimeEntry> _perfCounterDictionary = new ConcurrentDictionary<string, PerformanceTimeEntry>();

        public override void Debug(string message)
        {
#if NET45 || NET451
            Trace.TraceInformation(message);
#else
            Console.Error.WriteLine(message);
#endif
        }

        public override void Info(string message)
        {
#if NET45 || NET451
            Trace.TraceInformation(message);
#else
            Console.Error.WriteLine(message);
#endif
        }

        public override void Warn(string message)
        {
#if NET45 || NET451
            Trace.TraceWarning(message);
#else
            Console.Error.WriteLine(message);
#endif
        }

        public override void Error(string message, Exception exception = null)
        {
#if NET45 || NET451
            Trace.TraceError(message);
#else
            Console.Error.WriteLine(message);
#endif
        }

        private async Task PerformanceLogAsync()
        {
            StringBuilder message = new StringBuilder();
            while (true)
            {
                message.Clear();
                await Task.Delay(PerformanceLogInterval);
                message.AppendLine("Spanner performance metrics:");
                foreach (var kvp in _perfCounterDictionary)
                {
                    message.AppendLine($" {kvp.Key}  #={kvp.Value.Instances} avg={kvp.Value.Average} max={kvp.Value.Maximum} min={kvp.Value.Minimum} last={kvp.Value.Last}");
                }

#if NET45 || NET451
                Trace.TraceInformation(message.ToString());
#else
                Console.Error.WriteLine(message.ToString());
#endif
            }
            // ReSharper disable once FunctionNeverReturns
        }

        public override void LogPerformanceCounter(string name, double value)
        {
            if (Interlocked.CompareExchange(ref _perfLoggingTaskEnabled, 1, 0) == 0)
            {
                //kick off perf logging.
                Task.Run(PerformanceLogAsync);
            }

            var entry = _perfCounterDictionary.GetOrAdd(name, n => new PerformanceTimeEntry());

            lock (entry)
            {
                double total = entry.Instances * entry.Average;
                entry.Instances++;
                entry.Average = (total + value) / entry.Instances;
                entry.Maximum = Math.Max(entry.Maximum, value);
                entry.Minimum = Math.Min(entry.Minimum, value);
                entry.Last = value;
            }
        }

        class PerformanceTimeEntry
        {
            public int Instances { get; set; }
            public double Average { get; set; }
            public double Maximum { get; set; }
            public double Minimum { get; set; }
            public double Last { get; set; }
        }
    }
}