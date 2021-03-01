using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    public class Subscribe
    {
        [Fact]
        public async Task TestSubscribe()
        {

            var cts = new System.Threading.CancellationTokenSource();
            cts.CancelAfter(5000);

            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } }, };
            var client = new ClientAsync(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStream(streamName, "test.subscribe", cts.Token);

            var value = System.Text.Encoding.ASCII.GetBytes("hello, world");
            await client.PublishAsync(streamName, value, null, null, cts.Token);


            var sub = await client.Subscribe(streamName, new SubscriptionOptions
            {
                Partition = 0,
                StartPosition = Proto.StartPosition.Earliest,
            }, cts.Token);

            await foreach (var message in sub)
            {
                Assert.Equal("hello, world", System.Text.Encoding.UTF8.GetString(message.Value));
                break;
            }
            return;
        }

        [Fact]
        public async Task TestSubscribeSameStream()
        {

            var cts = new System.Threading.CancellationTokenSource();
            cts.CancelAfter(5000);

            var options = new ClientOptions { Brokers = new List<BrokerAddress> { new BrokerAddress { Host = "localhost", Port = 9292 }, new BrokerAddress { Host = "localhost", Port = 9393, } }, };
            var client = new ClientAsync(options);

            var streamName = Guid.NewGuid().ToString();
            await client.CreateStream(streamName, "test.subscribe", cts.Token);

            var value = System.Text.Encoding.ASCII.GetBytes("hello, world");
            await client.PublishAsync(streamName, value, null, null, cts.Token);


            var sub1 = await client.Subscribe(streamName, new SubscriptionOptions
            {
                Partition = 0,
                StartPosition = Proto.StartPosition.Earliest,
            }, cts.Token);
            var sub2 = await client.Subscribe(streamName, new SubscriptionOptions
            {
                Partition = 0,
                StartPosition = Proto.StartPosition.Earliest,
            }, cts.Token);

            Func<Liftbridge.Net.Subscription, Task> subHandler = async (sub) =>
            {
                await foreach (var message in sub)
                {
                    Assert.Equal("hello, world", System.Text.Encoding.UTF8.GetString(message.Value));
                    break;
                }
            };

            await Task.WhenAll(subHandler(sub1), subHandler(sub2));
            return;
        }
    }
}
