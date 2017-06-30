using System;
using System.Collections.Generic;
using System.Text;
using Google.Cloud.Spanner.V1;
using Xunit;

namespace Google.Cloud.Spanner.Data.Tests
{
    public class ConnectionStringBuilderTests
    {
        [Fact]
        public void DataSourceParsing()
        {
            var connectionStringBuilder =
                new SpannerConnectionStringBuilder(
                    "Data Source=projects/project1/instances/instance1/databases/database1");
            Assert.Equal("project1", connectionStringBuilder.Project);
            Assert.Equal("instance1", connectionStringBuilder.SpannerInstance);
            Assert.Equal("database1", connectionStringBuilder.SpannerDatabase);
        }

        [Fact]
        public void InstanceOnlyDataSource()
        {
            var connectionStringBuilder =
                new SpannerConnectionStringBuilder("Data Source=projects/project1/instances/instance1");
            Assert.Equal("project1", connectionStringBuilder.Project);
            Assert.Equal("instance1", connectionStringBuilder.SpannerInstance);
            Assert.Null(connectionStringBuilder.SpannerDatabase);
        }

        [Fact]
        public void BadDataSourceThrows1()
        {
            Assert.Throws<ArgumentException>( () =>
                new SpannerConnectionStringBuilder("Data Source=badjuju/project1/instances/instance1/databases/database1"));
        }

        [Fact]
        public void BadDataSourceThrows2()
        {
            Assert.Throws<ArgumentException>(() =>
               new SpannerConnectionStringBuilder("Data Source=projects/project1/badjuju/instance1/databases/database1"));
        }

        [Fact]
        public void BadDataSourceThrows3()
        {
            Assert.Throws<ArgumentException>(() =>
               new SpannerConnectionStringBuilder("Data Source=projects/project1/instances/instance1/badjuju/database1"));
        }

        [Fact]
        public void BadDataSourceThrows4()
        {
            Assert.Throws<ArgumentException>(() =>
               new SpannerConnectionStringBuilder("DataSource=projects/project1/instances/instance1/databases/database1"));
        }
        [Fact]
        public void BadDataSourceThrows5()
        {
            Assert.Throws<ArgumentException>(
                () =>
                    new SpannerConnectionStringBuilder
                    {
                        DataSource = "Data Source=projects/project1/instances/instance1/databases/database1"
                    }
            );
        }
        [Fact]
        public void BadDataSourceThrows6()
        {
            Assert.Throws<ArgumentException>(
                () =>
                    new SpannerConnectionStringBuilder
                    {
                        DataSource = "badjuju/project1/instances/instance1/databases/database1"
                    }
            );
        }
        [Fact]
        public void DefaultEndpointIfNotSpecified()
        {
            var connectionStringBuilder = new SpannerConnectionStringBuilder();
            Assert.Equal(SpannerClient.DefaultEndpoint.Host, connectionStringBuilder.Host);
            Assert.Equal(SpannerClient.DefaultEndpoint.Port, connectionStringBuilder.Port);
        }

        [Fact]
        public void EndpointParsing()
        {
            var connectionStringBuilder =
                new SpannerConnectionStringBuilder(
                    "Data Source=projects/project1/instances/instance1/databases/database1;Host=foo;Port=1234");

            Assert.Equal("foo", connectionStringBuilder.Host);
            Assert.Equal(1234, connectionStringBuilder.Port);
        }

        [Fact]
        public void EndPointSettableViaProperty()
        {
            var connectionStringBuilder = new SpannerConnectionStringBuilder();
            connectionStringBuilder.Host = "foo";
            connectionStringBuilder.Port = 1234;
            Assert.Equal("foo", connectionStringBuilder.Host);
            Assert.Equal(1234, connectionStringBuilder.Port);
        }

        [Fact]
        public void DataSourceSettableViaProperty()
        {
            var connectionStringBuilder = new SpannerConnectionStringBuilder();
            connectionStringBuilder.DataSource = "projects/project1/instances/instance1/databases/database1";
            Assert.Equal("project1", connectionStringBuilder.Project);
            Assert.Equal("instance1", connectionStringBuilder.SpannerInstance);
            Assert.Equal("database1", connectionStringBuilder.SpannerDatabase);
        }
    }
}
