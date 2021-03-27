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
            var client = new ClientAsync(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStream(streamName, "test");
            await client.DeleteStream(streamName);
            return;
        }

        [Fact]
        public async Task TestDeleteStreamNoNameAsync()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new ClientAsync(options);

            await Assert.ThrowsAsync<StreamNotExistsException>(async () => await client.DeleteStream(""));
            return;
        }

        [Fact]
        public async Task TestDeleteUpdatesMetadata()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new ClientAsync(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStream(streamName, "test");
            Assert.True(await client.StreamExists(streamName));
            await client.DeleteStream(streamName);
            Assert.False(await client.StreamExists(streamName));
            return;
        }
    }
}
