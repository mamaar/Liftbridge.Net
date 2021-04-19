using System;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    [Collection("Client collection")]
    public class DeleteStream
    {
        ClientFixture Fixture { get; }

        public DeleteStream(ClientFixture f)
        {
            Fixture = f;
        }

        [Fact]
        public async Task TestDeleteStreamAsync()
        {
            var streamName = Guid.NewGuid().ToString();
            await Fixture.Client.CreateStream(streamName, "test");
            await Fixture.Client.DeleteStream(streamName);
            return;
        }

        [Fact]
        public async Task TestDeleteStreamNoNameAsync()
        {
            await Assert.ThrowsAsync<StreamNotExistsException>(
                async () => await Fixture.Client.DeleteStream(""));
            return;
        }

        [Fact]
        public async Task TestDeleteUpdatesMetadata()
        {
            var streamName = Guid.NewGuid().ToString();
            await Fixture.Client.CreateStream(streamName, "test");
            Assert.True(await Fixture.Client.StreamExists(streamName));
            await Fixture.Client.DeleteStream(streamName);
            Assert.False(await Fixture.Client.StreamExists(streamName));
            return;
        }
    }
}
