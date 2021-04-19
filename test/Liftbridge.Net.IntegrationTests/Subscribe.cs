using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    [Collection("Client collection")]
    public class Subscribe
    {
        ClientFixture Fixture { get; }

        public Subscribe(ClientFixture f)
        {
            Fixture = f;
        }

        [Fact]
        public async Task TestSubscribe()
        {

            var cts = new System.Threading.CancellationTokenSource();
            cts.CancelAfter(5000);

            var streamName = Guid.NewGuid().ToString();
            await Fixture.Client.CreateStream(streamName, "test.subscribe", cts.Token);

            var value = System.Text.Encoding.ASCII.GetBytes("hello, world");
            await Fixture.Client.Publish(streamName, value, MessageOptions.Default, cts.Token);

            var sub = Fixture.Client.Subscribe(streamName, new SubscriptionOptions
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


            var streamName = Guid.NewGuid().ToString();
            await Fixture.Client.CreateStream(streamName, "test.subscribe", cts.Token);

            var value = System.Text.Encoding.ASCII.GetBytes("hello, world");
            await Fixture.Client.PublishAsync(streamName, value, null, null, cts.Token);


            var sub1 = Fixture.Client.Subscribe(streamName, new SubscriptionOptions
            {
                Partition = 0,
                StartPosition = Proto.StartPosition.Earliest,
            }, cts.Token);
            var sub2 = Fixture.Client.Subscribe(streamName, new SubscriptionOptions
            {
                Partition = 0,
                StartPosition = Proto.StartPosition.Earliest,
            }, cts.Token);

            Func<IAsyncEnumerable<Message>, Task> subHandler = async (sub) =>
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
