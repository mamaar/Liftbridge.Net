using System;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    [Collection("Client collection")]
    public class SetStreamReadonly
    {
        ClientFixture Fixture { get; }

        public SetStreamReadonly(ClientFixture f)
        {
            Fixture = f;
        }

        [Fact]
        public async Task TestSetStreamReadonlyAsync()
        {
            var streamName = Guid.NewGuid().ToString();
            await Fixture.Client.CreateStream(streamName, "test");
            await Fixture.Client.SetStreamReadonly(streamName);
            return;
        }

        [Fact]
        public async Task TestSetStreamReadonlyNoNameAsync()
        {
            await Assert.ThrowsAsync<StreamNotExistsException>(
                async () => await Fixture.Client.SetStreamReadonly("")
                );
            return;
        }
    }
}
