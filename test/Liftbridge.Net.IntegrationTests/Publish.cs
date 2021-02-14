using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    public class Publish
    {
        [Fact]
        public async Task TestPublish()
        {
            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } }, AckWaitTime = new TimeSpan(0, 0, 0, 0, 1), };
            var client = new ClientAsync(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStream(streamName, "test");

            var value = System.Text.Encoding.ASCII.GetBytes("hello, world");
            var ack = await client.Publish(streamName, value, new MessageOptions { });
            Assert.Equal(Proto.Ack.Types.Error.Ok, ack.AckError);
            return;
        }
    }
}
