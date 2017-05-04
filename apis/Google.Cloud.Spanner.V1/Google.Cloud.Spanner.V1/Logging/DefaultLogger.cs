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
        private const int PerformanceLogInterval = 10000;
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
                    //to make the tavg correct, we re-record the last value at the current timestamp.
                    RecordEntryValue(kvp.Value, () => kvp.Value.Last );

                    //log it.
                    message.AppendLine($" {kvp.Key}  #={kvp.Value.Instances} tavg={kvp.Value.TimeWeightedAverage} avg={kvp.Value.Average} max={kvp.Value.Maximum} min={kvp.Value.Minimum} last={kvp.Value.Last}");
                    
                    //now reset to last.
                    lock (kvp.Value)
                    {
                        kvp.Value.TotalTime = default(TimeSpan);
                        kvp.Value.LastMeasureTime = DateTime.UtcNow;
                        kvp.Value.Maximum = kvp.Value.Last;
                        kvp.Value.Minimum = kvp.Value.Last;
                        kvp.Value.Average = kvp.Value.Last;
                        kvp.Value.TimeWeightedAverage = kvp.Value.Last;
                    }
                }

#if NET45 || NET451
                Trace.TraceInformation(message.ToString());
#else
                Console.Error.WriteLine(message.ToString());
#endif
            }
            // ReSharper disable once FunctionNeverReturns
        }

        private void RecordEntryValue(PerformanceTimeEntry entry, Func<double> valueFunc)
        {
            lock (entry)
            {
                var value = valueFunc();
                double total = entry.Instances * entry.Average;
                entry.Instances++;
                entry.Average = (total + value) / entry.Instances;
                entry.Maximum = Math.Max(entry.Maximum, value);
                entry.Minimum = Math.Min(entry.Minimum, value);
                var now = DateTime.UtcNow;
                if (entry.LastMeasureTime != default(DateTime))
                {
                    double milliSoFar = 0;
                    var deltaTime = (now - entry.LastMeasureTime).TotalMilliseconds;
                    milliSoFar += entry.TotalTime.TotalMilliseconds;
                    entry.TotalTime = entry.TotalTime.Add(TimeSpan.FromMilliseconds(deltaTime));
                    entry.TimeWeightedAverage = (milliSoFar * entry.TimeWeightedAverage + entry.Last * deltaTime) /
                                                entry.TotalTime.TotalMilliseconds;
                }
                entry.LastMeasureTime = now;
                entry.Last = value;
            }
        }

        public override void LogPerformanceCounter(string name, double value)
        {
            if (Interlocked.CompareExchange(ref _perfLoggingTaskEnabled, 1, 0) == 0)
            {
                //kick off perf logging.
                Task.Run(PerformanceLogAsync);
            }

            RecordEntryValue(_perfCounterDictionary.GetOrAdd(name, n => new PerformanceTimeEntry()), () => value);
        }

        class PerformanceTimeEntry
        {
            public int Instances { get; set; }
            public double Average { get; set; }
            public double TimeWeightedAverage { get; set; }
            public TimeSpan TotalTime { get; set; }
            public double Maximum { get; set; }
            public double Minimum { get; set; }
            public double Last { get; set; }
            public DateTime LastMeasureTime { get; set; }
        }
    }
}