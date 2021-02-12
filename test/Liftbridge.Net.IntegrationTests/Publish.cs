using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    public class Publish
    {
        [Fact]
        public async Task TestPublishNoAck()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } } };
            var client = new Client(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStreamAsync(streamName, "test");

            var value = System.Text.Encoding.ASCII.GetBytes("hello, world");
            await client.PublishAsync(streamName, value);
            return;
        }
    }
}
