using System;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    [Collection("Client collection")]
    public class PauseStream
    {
        ClientFixture Fixture { get; }

        public PauseStream(ClientFixture f)
        {
            Fixture = f;
        }

        [Fact]
        public async Task TestPauseStreamAsync()
        {
            var streamName = Guid.NewGuid().ToString();
            await Fixture.Client.CreateStream(streamName, "test");
            await Fixture.Client.PauseStream(streamName);
            return;
        }

        [Fact]
        public async Task TestPauseStreamNoNameAsync()
        {
            await Assert.ThrowsAsync<StreamNotExistsException>(
                async () => await Fixture.Client.PauseStream("")
            );
            return;
        }
    }
}
