using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    public class PauseStream
    {
        [Fact]
        public async Task TestPauseStreamAsync()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new Client(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStreamAsync(streamName, "test");
            await client.PauseStreamAsync(streamName);
            return;
        }

        [Fact]
        public void TestPauseStream()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new Client(options);

            var streamName = Guid.NewGuid().ToString();
            client.CreateStream(streamName, "test");
            client.PauseStream(streamName);
            return;
        }

        [Fact]
        public void TestPauseStreamNoName()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new Client(options);

            Assert.Throws<StreamNotExistsException>(() => client.PauseStream(""));
        }

        [Fact]
        public async Task TestPauseStreamNoNameAsync()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new Client(options);

            await Assert.ThrowsAsync<StreamNotExistsException>(async () => await client.PauseStreamAsync(""));
            return;
        }
    }
}
