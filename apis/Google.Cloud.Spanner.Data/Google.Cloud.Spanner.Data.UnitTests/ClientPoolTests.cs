using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Api.Gax.Grpc;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.V1.Logging;
using Moq;
using Xunit;
using Xunit.Abstractions;

namespace Google.Cloud.Spanner.Data.Tests
{
    public class ClientPoolTests
    {
        public ClientPoolTests(ITestOutputHelper outputHelper)
        {
            TestLogger.TestOutputHelper = outputHelper;
            TestLogger.Install();
            Logger.LogLevel = V1.Logging.LogLevel.Debug;
        }
        private static Mock<ISpannerClientFactory> SetupMockClientFactory(Mock<SpannerClient> mockClient)
        {
            var mockClientFactory = new Mock<ISpannerClientFactory>();
            mockClientFactory.Setup(
                    x => x.CreateClientAsync(It.IsAny<ServiceEndpoint>(), It.IsAny<ITokenAccess>()))
                .ReturnsAsync(
                    () =>
                    {
                        //return a unique instance each time.
                        var result = mockClient.Object;
                        mockClient = new Mock<SpannerClient>();
                        return result;
                    });
            return mockClientFactory;
        }

        [Fact]
        public async Task ClientsCreatedOnDemandAsync()
        {
            //SpannerClients should not be precreated, but be created lazily.
            var firstReturnedClient = new Mock<SpannerClient>();
            var mockClientFactory = SetupMockClientFactory(firstReturnedClient);

            var testPool = new ClientPool(mockClientFactory.Object);
            var clientAcquired = await testPool.AcquireClientAsync();
            testPool.ReleaseClient(clientAcquired);

            mockClientFactory.Verify(x => x.CreateClientAsync(It.IsAny<ServiceEndpoint>(), It.IsAny<ITokenAccess>()), Times.Exactly(1));
            var s = new StringBuilder();
            Assert.Equal(0, testPool.GetPoolInfo(s));
            Logger.Instance.Info(s.ToString());
        }

        [Fact]
        public async Task SequentialCreatesOnlyUseOneChannel()
        {
            //SpannerClients if created sequentially should only use a single channel.
            //This is to faciliate good session pool hits (which is per channel).  If we always
            //round robin'd clients, then we would get hard cache misses until we populated all
            //of the caches per channel.
            //Since channels are lazily created, this also saves resources for scenarios like rich clients
            //that may only need a single session for multiple sequential workloads.
            //Our default setting for # channels = 4 needs to work well for all scenarios.
            var firstReturnedClient = new Mock<SpannerClient>();
            var mockClientFactory = SetupMockClientFactory(firstReturnedClient);

            var testPool = new ClientPool(mockClientFactory.Object);
            var expectedClient = firstReturnedClient.Object;

            Assert.Same(expectedClient, await testPool.AcquireClientAsync());
            testPool.ReleaseClient(expectedClient);

            Assert.Equal(expectedClient, await testPool.AcquireClientAsync());
            testPool.ReleaseClient(expectedClient);

            Assert.Equal(expectedClient, await testPool.AcquireClientAsync());
            testPool.ReleaseClient(expectedClient);

            var s = new StringBuilder();
            Assert.Equal(0, testPool.GetPoolInfo(s));
            Logger.Instance.Info(s.ToString());
            mockClientFactory.Verify(x => x.CreateClientAsync(It.IsAny<ServiceEndpoint>(), It.IsAny<ITokenAccess>()), Times.Exactly(1));
        }

        [Fact]
        public async Task RoundRobin()
        {
            var firstReturnedClient = new Mock<SpannerClient>();
            var mockClientFactory = SetupMockClientFactory(firstReturnedClient);

            var testPool = new ClientPool(mockClientFactory.Object);

            List<SpannerClient> clientList = new List<SpannerClient>();
            for (var i = 0; i < SpannerOptions.Instance.MaximumGrpcChannels; i++)
            {
                var newClient = await testPool.AcquireClientAsync();
                foreach (var existing in clientList)
                {
                    Assert.NotSame(existing, newClient);
                }
                clientList.Add(newClient);
            }

            //now we wrap around.
            var firstReusedClient = await testPool.AcquireClientAsync();
            Assert.Same(clientList[0], firstReusedClient);
            testPool.ReleaseClient(firstReusedClient);

            foreach (var client in clientList)
            {
                testPool.ReleaseClient(client);
            }
            var s = new StringBuilder();
            Assert.Equal(0, testPool.GetPoolInfo(s));
            Logger.Instance.Info(s.ToString());

            //now that everything got released, lets ensure the client order is preserved so
            //that we again get the first client.
            Assert.Same(firstReusedClient, await testPool.AcquireClientAsync());
            testPool.ReleaseClient(firstReusedClient);

            mockClientFactory.Verify(x => x.CreateClientAsync(It.IsAny<ServiceEndpoint>(), It.IsAny<ITokenAccess>()),
                Times.Exactly(SpannerOptions.Instance.MaximumGrpcChannels));
        }

        private async Task<SpannerClient> GetSpannerClientAsync(ClientPool pool)
        {
            //immediately yield to increase contention for stress testing.
            await Task.Yield();
            return await pool.AcquireClientAsync();
        }

        [Fact]
        public async Task ConcurrencyStress()
        {
            const int multiplier = 10;
            var firstReturnedClient = new Mock<SpannerClient>();
            var mockClientFactory = SetupMockClientFactory(firstReturnedClient);

            //A mini stress test that hits the client pool with multiple concurrent client requests.
            List<Task<SpannerClient>> concurrentQueries = new List<Task<SpannerClient>>();
            var testPool = new ClientPool(mockClientFactory.Object);

            for (int i = 0; i < SpannerOptions.Instance.MaximumGrpcChannels * multiplier; i++)
            {
                concurrentQueries.Add(GetSpannerClientAsync(testPool));
            }

            await Task.WhenAll(concurrentQueries);
            mockClientFactory.Verify(x => x.CreateClientAsync(It.IsAny<ServiceEndpoint>(), It.IsAny<ITokenAccess>()),
                Times.Exactly(SpannerOptions.Instance.MaximumGrpcChannels));

            var grouping = concurrentQueries.GroupBy(x => x.Result).ToList();
            Assert.Equal(SpannerOptions.Instance.MaximumGrpcChannels, grouping.Count());
            foreach (var group in grouping)
            {
                Assert.Equal(multiplier, group.Count());
            }

            foreach (var client in concurrentQueries.Select(x => x.Result))
            {
                testPool.ReleaseClient(client);
            }
            var s = new StringBuilder();
            Assert.Equal(0, testPool.GetPoolInfo(s));
            Logger.Instance.Info(s.ToString());
        }
    }
}
