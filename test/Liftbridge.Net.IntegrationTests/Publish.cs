using System;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    [Collection("Client collection")]
    public class Publish
    {
        ClientFixture Fixture { get; }

        public Publish(ClientFixture f)
        {
            Fixture = f;
        }

        [Fact]
        public async Task TestPublish()
        {
            var streamName = Guid.NewGuid().ToString();
            await Fixture.Client.CreateStream(streamName, "test");

            var value = System.Text.Encoding.ASCII.GetBytes("hello, world");
            var ack = await Fixture.Client.Publish(streamName, value, new MessageOptions { });
            Assert.Equal(Proto.Ack.Types.Error.Ok, ack.AckError);
            return;
        }

        [Fact]
        public async Task TestPublishToSubject()
        {
            var subject = "test.to.subject";
            var streamName = Guid.NewGuid().ToString();
            await Fixture.Client.CreateStream(streamName, subject);

            var value = System.Text.Encoding.ASCII.GetBytes("hello, world");
            await Fixture.Client.PublishToSubject(subject, value, MessageOptions.Default);
            return;
        }
    }
}
