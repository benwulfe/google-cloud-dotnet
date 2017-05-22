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

using System;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.Spanner.Data.Tests
{
    public class ReadTests : IClassFixture<TestDatabaseFixture>
    {
        private readonly TestDatabaseFixture _testFixture;

        public ReadTests(TestDatabaseFixture testFixture) {
            _testFixture = testFixture;
        }

        [Fact]
        public async Task TestReadyEmpty() {
            // ReSharper disable once RedundantAssignment
            int rowsRead = -1;

            using (var connection = await _testFixture.GetTestDatabaseConnectionAsync()) {
                var cmd = connection.CreateSelectCommand("SELECT * FROM "  + _testFixture.TestTable + " WHERE Key >= 'k99'");
                using (var reader = await cmd.ExecuteReaderAsync()) {
                    rowsRead = 0;
                    while (await reader.ReadAsync()) {
                        rowsRead++;
                    }
                }
            }
            Assert.True(rowsRead == 0);
        }

        [Fact]
        public async Task TestPointRead()
        {
            // ReSharper disable once RedundantAssignment
            int rowsRead = -1;

            using (var connection = await _testFixture.GetTestDatabaseConnectionAsync())
            {
                var cmd = connection.CreateSelectCommand("SELECT * FROM " + _testFixture.TestTable + " WHERE Key = 'k1'");
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    rowsRead = 0;
                    while (await reader.ReadAsync())
                    {
                        Assert.True(reader.GetString(0) == "k1");
                        Assert.True(reader.GetString(1) == "v1");
                        rowsRead++;

                    }
                }
            }
            Assert.True(rowsRead == 1);
        }

        [Fact]
        public async Task TestPointReadEmpty()
        {
            // ReSharper disable once RedundantAssignment
            int rowsRead = -1;

            using (var connection = await _testFixture.GetTestDatabaseConnectionAsync())
            {
                var cmd = connection.CreateSelectCommand("SELECT * FROM " + _testFixture.TestTable + " WHERE Key = 'k99'");
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    rowsRead = 0;
                    while (await reader.ReadAsync())
                    {
                        rowsRead++;

                    }
                }
            }
            Assert.True(rowsRead == 0);
        }

        [Fact]
        public async Task TestBadDbName()
        {
            string connectionString = "Data Source=" + _testFixture.TestProjectName + "/" + _testFixture.TestInstanceName + "/"
                                          + "badjuju";
            // ReSharper disable once RedundantAssignment
            int rowsRead = -1;
            bool exceptionCaught = false;

            try {
                using (var connection = new SpannerConnection(connectionString)) {
                    var cmd =
                        connection.CreateSelectCommand("SELECT * FROM " + _testFixture.TestTable
                                                       + " WHERE Key = 'k1'");
                    using (var reader = await cmd.ExecuteReaderAsync()) {
                        rowsRead = 0;
                        while (await reader.ReadAsync()) {
                            rowsRead++;

                        }
                    }
                }
            } catch (SpannerException e) {
                exceptionCaught = true;
                Assert.True(e.ErrorCode == ErrorCode.NotFound);
                Assert.False(e.IsTransientSpannerFault());
            }

            Assert.True(exceptionCaught);
            Assert.True(rowsRead == -1);
        }

        [Fact]
        public async Task TestBadTableName()
        {
            // ReSharper disable once RedundantAssignment
            int rowsRead = -1;
            bool exceptionCaught = false;

            try {
                using (var connection = await _testFixture.GetTestDatabaseConnectionAsync()) {
                    var cmd =
                        connection.CreateSelectCommand("SELECT * FROM badjuju WHERE Key = 'k99'");
                    using (var reader = await cmd.ExecuteReaderAsync()) {
                        rowsRead = 0;
                        while (await reader.ReadAsync()) {
                            rowsRead++;

                        }
                    }
                }
            } catch (SpannerException e) {
                exceptionCaught = true;
                Assert.True(e.ErrorCode == ErrorCode.InvalidArgument);
                Assert.False(e.IsTransientSpannerFault());
            }

            Assert.True(exceptionCaught);
            Assert.True(rowsRead == 0);
        }
    }
}