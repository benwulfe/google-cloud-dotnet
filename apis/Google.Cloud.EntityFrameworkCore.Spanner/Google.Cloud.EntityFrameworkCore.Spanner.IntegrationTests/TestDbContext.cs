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

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    /// <summary>
    /// 
    /// </summary>
    public class TestDbContext : DbContext
    {
        private readonly TestDatabaseFixture _fixture;
        private readonly Predicate<string> _logFilter;
        private readonly StringBuilder _log = new StringBuilder();

        public TestDbContext(TestDatabaseFixture fixture, Predicate<string> logFilter)
        {
            _fixture = fixture;
            _logFilter = logFilter;
        }

        public virtual DbSet<StringEntry> StringTable { get; set; }
        public virtual DbSet<Book> BookTable { get; set; }
        public virtual DbSet<Author> AuthorTable { get; set; }
        public virtual DbSet<TypeEntry> TypeTable { get; set; }

        public string GetCurrentLog()
        {
            lock (_log)
            {
                return _log.ToString().Trim();
            }
        }

        public void ClearLog()
        {
            lock (_log)
            {
                _log.Clear();
            }
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSpanner(_fixture.ConnectionString)
            .EnableSensitiveDataLogging()
            .UseLoggerFactory(new LoggerFactory(this));
        }

        class LoggerFactory : ILoggerFactory
        {
            private readonly TestDbContext _testDbContext;

            public LoggerFactory(TestDbContext testDbContext)
            {
                _testDbContext = testDbContext;
            }

            public void Dispose()
            {
            }
            public ILogger CreateLogger(string categoryName) => new RecordedLogger(_testDbContext);
            public void AddProvider(ILoggerProvider provider)
            {}
        }

        class RecordedLogger : ILogger
        {
            private readonly TestDbContext _testDbContext;
            public RecordedLogger(TestDbContext testDbContext)
            {
                _testDbContext = testDbContext;
            }

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                string message = formatter(state, exception).Replace("\r\n", " ").Trim();
                if (logLevel >= LogLevel.Error || _testDbContext._logFilter(message))
                {
                    lock (_testDbContext._log)
                    {
                        _testDbContext._log.AppendLine(
                            $"message:{message}");
                    }
                }
            }

            public bool IsEnabled(LogLevel logLevel) => true;

            public IDisposable BeginScope<TState>(TState state)
            {
                return null;
            }
        }
    }

    public class StringEntry
    {
        [Key]
        public string Key { get; set; }
        public string StringValue { get; set; }
    }

    public class Book
    {
        [Key]
        public string ISBN { get; set; }
        public string Title { get; set; }
        public string AuthorId { get; set; }
        [Column(TypeName="DATE")]
        public DateTime PublishDate { get; set; }
    }

    public class Author
    {
        [Key]
        public string AuthorId { get; set; }
        public string LastName { get; set; }
        public string FirstName { get; set; }
    }

    public class TypeEntry
    {
        /*                  K                   STRING(MAX) NOT NULL,
                  BoolValue           BOOL,
                  Int64Value          INT64,
                  Float64Value        FLOAT64,
                  StringValue         STRING(MAX),
                  BytesValue          BYTES(MAX),
                  TimestampValue      TIMESTAMP,
                  DateValue           DATE,
                  BoolArrayValue      ARRAY<BOOL>,
                  Int64ArrayValue     ARRAY<INT64>,
                  Float64ArrayValue   ARRAY<FLOAT64>,
                  StringArrayValue    ARRAY<STRING(MAX)>,
                  BytesArrayValue     ARRAY<BYTES(MAX)>,
                  TimestampArrayValue ARRAY<TIMESTAMP>,
                  DateArrayValue      ARRAY<DATE>,
*/
        [Key]
        public string K { get; set; }
        public string BoolValue { get; set; }
        public string Int64Value { get; set; }
        public string Float64Value { get; set; }
        public string StringValue { get; set; }
        public string BytesValue { get; set; }
        public string TimestampValue { get; set; }
        public string DateValue { get; set; }
        public string BoolArrayValue { get; set; }
        public string Int64ArrayValue { get; set; }
        public string Float64ArrayValue { get; set; }
        public string StringArrayValue { get; set; }
        public string BytesArrayValue { get; set; }
        public string TimestampArrayValue { get; set; }
        public string DateArrayValue { get; set; }
    }
}