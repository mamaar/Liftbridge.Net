using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    public class SetFetchCursor
    {
        [Fact]
        public async Task TestSetAndFetchCursor()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new ClientAsync(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStream(streamName, "test");

            await client.SetCursor("my-cursor", streamName, 0, 12);
            var offset = await client.FetchCursor("my-cursor", streamName, 0);

            Assert.Equal(12, offset);

            return;
        }
    }
}
