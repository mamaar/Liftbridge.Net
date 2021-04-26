using System;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    [Collection("Client collection")]
    public class SetFetchCursor
    {
        ClientFixture Fixture { get; }

        public SetFetchCursor(ClientFixture f)
        {
            Fixture = f;
        }

        [Fact]
        public async Task TestSetAndFetchCursor()
        {
            var streamName = Guid.NewGuid().ToString();
            await Fixture.Client.CreateStream(streamName, "test");

            await Assert.ThrowsAsync<CursorsDisabledException>(() => Fixture.Client.SetCursor("my-cursor", streamName, 0, 12));
            await Assert.ThrowsAsync<CursorsDisabledException>(() => Fixture.Client.FetchCursor("my-cursor", streamName, 0));

            return;
        }
    }
}
