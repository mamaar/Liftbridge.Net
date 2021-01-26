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
            var client = new Client(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStreamAsync(streamName, "test");
            return;
        }

        [Fact]
        public async Task TestCreateStreamWithSameNameAsync()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new Client(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStreamAsync(streamName, "test.samename.async.first");
            //await Task.Delay(500);
            await Assert.ThrowsAsync<StreamAlreadyExistsException>(() => client.CreateStreamAsync(streamName, "test.samename.async.second"));
            return;
        }

        [Fact]
        public void TestCreateNewStream()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new Client(options);

            var streamName = Guid.NewGuid().ToString();
            client.CreateStream(streamName, "test");
            return;
        }

        [Fact]
        public void TestCreateStreamWithSameName()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new Client(options);

            var streamName = Guid.NewGuid().ToString();
            client.CreateStream(streamName, "test.samename.first");
            //Thread.Sleep(500);
            Assert.Throws<StreamAlreadyExistsException>(() => client.CreateStream(streamName, "test.samename.second"));
            return;
        }
    }
}
