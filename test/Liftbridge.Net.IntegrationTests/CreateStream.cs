using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    public class CreateStream
    {
        [Fact]
        public async Task TestCreateNewStreamAsync()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new ClientAsync(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStream(streamName, "test");
            return;
        }

        [Fact]
        public async Task TestCreateStreamWithSameNameAsync()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new ClientAsync(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStream(streamName, "test.samename.async.first");
            await Assert.ThrowsAsync<StreamAlreadyExistsException>(() => client.CreateStream(streamName, "test.samename.async.second"));
            return;
        }

        [Fact]
        public async Task TestStreamExists()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new ClientAsync(options);

            var streamName = Guid.NewGuid().ToString();
            var preCreateResult = await client.StreamExists(streamName);
            await client.CreateStream(streamName, "test");
            var postCreateResult = await client.StreamExists(streamName);

            Assert.False(preCreateResult);
            Assert.True(postCreateResult);
            return;
        }
    }
}
