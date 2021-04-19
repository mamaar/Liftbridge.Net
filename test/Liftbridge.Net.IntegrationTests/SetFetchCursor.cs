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

            await Fixture.Client.SetCursor("my-cursor", streamName, 0, 12);
            var offset = await Fixture.Client.FetchCursor("my-cursor", streamName, 0);

            Assert.Equal(12, offset);

            return;
        }
    }
}
