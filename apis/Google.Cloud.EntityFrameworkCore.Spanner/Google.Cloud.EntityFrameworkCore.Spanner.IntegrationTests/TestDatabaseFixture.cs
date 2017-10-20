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

#region

using System;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Cloud.Spanner.Data;
using Xunit;

#endregion

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    // ReSharper disable once ClassNeverInstantiated.Global
    [CollectionDefinition(nameof(TestDatabaseFixture))]
    public class TestDatabaseFixture : IDisposable, ICollectionFixture<TestDatabaseFixture>
    {
        private readonly Lazy<Task> _creationTask;
        public string TestInstanceName => "spannerintegration";
        public string TestProjectName => Environment.GetEnvironmentVariable("TEST_PROJECT") ?? "cloud-sharp-jenkins";
        public string ConnectionString => $"{NoDbConnectionString}/databases/{DatabaseName}";
        public string NoDbConnectionString => $"Data Source=projects/{TestProjectName}/instances/{TestInstanceName}";

        // scratch can be used to run tests on a precreated db.
        // all tests are designed to be re-run on an existing db (previously written state will
        // not cause failures).
        // if you use a scratch database, be sure to comment out the database
        // creation and disposal methods.
        private string DatabaseName { get; }  =  // "scratch";
            "t_" + Guid.NewGuid().ToString("N").Substring(0, 28);
        public int TestTableRowCount { get; } = 15;
        public int BookAuthorRowCount { get; } = 25;
        public int TypeRowCount { get; } = 100;

        public string TestTableName { get; } = nameof(TestDbContext.StringTable);
        public string BookTableName { get; } = nameof(TestDbContext.BookTable);
        public string AuthorTableName { get; } = nameof(TestDbContext.AuthorTable);
        public string TypeTableName { get; } = nameof(TestDbContext.AuthorTable);

        public TestDatabaseFixture() => _creationTask = new Lazy<Task>(EnsureTestDatabaseImplAsync);

        public void Dispose()
        {
            using (var connection = new SpannerConnection(NoDbConnectionString))
            {
                var dropCommand = connection.CreateDdlCommand("DROP DATABASE " + DatabaseName);
                dropCommand.ExecuteNonQueryAsync().ResultWithUnwrappedExceptions();
            }
        }
        
        private async Task ExecuteDdlAsync(string ddlStatement)
        {
            using (var connection = new SpannerConnection(ConnectionString))
            {
                var createCmd = connection.CreateDdlCommand(ddlStatement);
                await createCmd.ExecuteNonQueryAsync();
            }
        }

        public Task EnsureTestDatabaseAsync() => _creationTask.Value;

        private async Task EnsureTestDatabaseImplAsync()
        {
            await Task.Delay(1); // this line exists so the others can be commented.
            await CreateDatabaseAsync();
            await Task.WhenAll(
                CreateBaseTableAsync(),
                CreateBooksTableAsync(),
                CreateAuthorsTableAsync(),
                CreateTypeTableAsync()).ConfigureAwait(false);
            await Task.WhenAll(
                FillBaseTableAsync(),
                FillAuthorsAsync(),
                FillBooksAsync()
                );
        }

        private async Task CreateDatabaseAsync()
        {
            using (var connection = new SpannerConnection(NoDbConnectionString))
            {
                var createCmd = connection.CreateDdlCommand("CREATE DATABASE " + DatabaseName);
                await createCmd.ExecuteNonQueryAsync();
            }
        }

        private async Task CreateBaseTableAsync()
        {
            string createTableStatement = $@"CREATE TABLE {TestTableName} (
                                            Key                STRING(MAX) NOT NULL,
                                            StringValue        STRING(MAX),
                                          ) PRIMARY KEY (Key)";

            var index1 = "CREATE INDEX TestTableByValue ON TestTable(StringValue)";
            var index2 = "CREATE INDEX TestTableByValueDesc ON TestTable(StringValue DESC)";

            await ExecuteDdlAsync(createTableStatement);
            await ExecuteDdlAsync(index1);
            await ExecuteDdlAsync(index2);
        }

        private async Task FillBaseTableAsync()
        {
            using (var connection = new SpannerConnection(ConnectionString))
            {
                await connection.OpenAsync();
                using (var tx = await connection.BeginTransactionAsync())
                {
                    var cmd = connection.CreateInsertCommand(
                        TestTableName,
                        new SpannerParameterCollection
                        {
                            {"Key", SpannerDbType.String},
                            {"StringValue", SpannerDbType.String}
                        });
                    cmd.Transaction = tx;

                    for (var i = 0; i < TestTableRowCount; ++i)
                    {
                        cmd.Parameters["Key"].Value = "k" + i;
                        cmd.Parameters["StringValue"].Value = "v" + i;
                        await cmd.ExecuteNonQueryAsync();
                    }

                    await tx.CommitAsync();
                }
            }
        }

        private async Task CreateBooksTableAsync()
        {
            var typeTable = $@"CREATE TABLE {BookTableName} ( 
                  ISBN                STRING(MAX) NOT NULL,
                  Title               STRING(MAX) NOT NULL,
                  AuthorId            STRING(MAX),
                  PublishDate         DATE,
                ) PRIMARY KEY (ISBN)";

            await ExecuteDdlAsync(typeTable);
        }

        private async Task FillBooksAsync()
        {
            using (var connection = new SpannerConnection(ConnectionString))
            {
                await connection.OpenAsync();
                using (var tx = await connection.BeginTransactionAsync())
                {
                    var cmd = connection.CreateInsertCommand(
                        BookTableName,
                        new SpannerParameterCollection
                        {
                            {"ISBN", SpannerDbType.String},
                            {"Title", SpannerDbType.String},
                            {"AuthorId", SpannerDbType.String},
                            {"PublishDate", SpannerDbType.Date}
                        });
                    cmd.Transaction = tx;

                    for (var i = 0; i < BookAuthorRowCount; ++i)
                    {
                        cmd.Parameters["ISBN"].Value = "ISBN" + i;
                        cmd.Parameters["Title"].Value = "Title" + i;
                        cmd.Parameters["AuthorId"].Value = "AuthorId" + i;
                        cmd.Parameters["PublishDate"].Value = DateTime.Now;
                        await cmd.ExecuteNonQueryAsync();
                    }

                    await tx.CommitAsync();
                }
            }
        }

        private async Task CreateAuthorsTableAsync()
        {
            var typeTable = $@"CREATE TABLE {AuthorTableName} ( 
                  AuthorId            STRING(MAX) NOT NULL,
                  LastName            STRING(MAX),
                  FirstName           STRING(MAX),
                ) PRIMARY KEY (AuthorId)";

            await ExecuteDdlAsync(typeTable);
        }

        private async Task FillAuthorsAsync()
        {
            using (var connection = new SpannerConnection(ConnectionString))
            {
                await connection.OpenAsync();
                using (var tx = await connection.BeginTransactionAsync())
                {
                    var cmd = connection.CreateInsertCommand(
                        AuthorTableName,
                        new SpannerParameterCollection
                        {
                            {"AuthorId", SpannerDbType.String},
                            {"LastName", SpannerDbType.String},
                            {"FirstName", SpannerDbType.String},
                        });
                    cmd.Transaction = tx;

                    for (var i = 0; i < BookAuthorRowCount; ++i)
                    {
                        cmd.Parameters["AuthorId"].Value = "AuthorId" + i;
                        cmd.Parameters["LastName"].Value = "LastName" + i;
                        cmd.Parameters["FirstName"].Value = "FirstName" + i;
                        await cmd.ExecuteNonQueryAsync();
                    }

                    await tx.CommitAsync();
                }
            }
        }

        private async Task CreateTypeTableAsync()
        {
            var typeTable = $@"CREATE TABLE {TypeTableName} ( 
                  K                   STRING(MAX) NOT NULL,
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
                ) PRIMARY KEY (K)";

            await ExecuteDdlAsync(typeTable);
        }

        private async Task FillTypeTableAsync()
        {
            using (var connection = new SpannerConnection(ConnectionString))
            {
                await connection.OpenAsync();
                using (var tx = await connection.BeginTransactionAsync())
                {
                    var cmd = connection.CreateInsertCommand(
                        TypeTableName,
                        new SpannerParameterCollection
                        {
                            {"K", SpannerDbType.String},
                            {"BoolValue", SpannerDbType.Bool},
                            {"Int64Value", SpannerDbType.Int64},
                            {"Float64Value", SpannerDbType.Float64},
                            {"StringValue", SpannerDbType.String},
                            {"BytesValue", SpannerDbType.Bytes},
                            {"TimestampValue", SpannerDbType.Timestamp},
                            {"DateValue", SpannerDbType.Date},
                            {"BoolArrayValue", SpannerDbType.ArrayOf(SpannerDbType.Bool)},
                            {"Int64ArrayValue", SpannerDbType.ArrayOf(SpannerDbType.Int64)},
                            {"Float64ArrayValue", SpannerDbType.ArrayOf(SpannerDbType.Float64)},
                            {"StringArrayValue", SpannerDbType.ArrayOf(SpannerDbType.String)},
                            {"BytesArrayValue", SpannerDbType.ArrayOf(SpannerDbType.Bytes)},
                            {"TimestampArrayValue", SpannerDbType.ArrayOf(SpannerDbType.Timestamp)},
                            {"DateArrayValue", SpannerDbType.ArrayOf(SpannerDbType.Date)},
                        });
                    cmd.Transaction = tx;

                    for (var i = 0; i < BookAuthorRowCount; ++i)
                    {
                        cmd.Parameters["K"].Value = "AuthorId" + i;
                        cmd.Parameters["K"].Value = "AuthorId" + i;
                        cmd.Parameters["K"].Value = "AuthorId" + i;
                        cmd.Parameters["K"].Value = "AuthorId" + i;
                        cmd.Parameters["K"].Value = "AuthorId" + i;
                        cmd.Parameters["K"].Value = "AuthorId" + i;
                        cmd.Parameters["K"].Value = "AuthorId" + i;
                        cmd.Parameters["K"].Value = "AuthorId" + i;
                        await cmd.ExecuteNonQueryAsync();
                    }

                    await tx.CommitAsync();
                }
            }
        }

        public async Task<TestDbContext> CreateContextAsync(Predicate<string> logFilter = null)
        {
            logFilter = logFilter ?? (x => true);
            await EnsureTestDatabaseAsync();
            return new TestDbContext(this, logFilter);
        }
    }
}
