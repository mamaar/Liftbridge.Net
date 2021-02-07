using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    public class DeleteStream
    {
        [Fact]
        public async Task TestDeleteStreamAsync()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new Client(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStreamAsync(streamName, "test");
            await client.DeleteStreamAsync(streamName);
            return;
        }

        [Fact]
        public async Task TestDeleteStreamNoNameAsync()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new Client(options);

            await Assert.ThrowsAsync<StreamNotExistsException>(async () => await client.DeleteStreamAsync(""));
            return;
        }
    }
}
