using System;
using System.Threading.Tasks;
using Xunit;

namespace Liftbridge.Net.IntegrationTests
{
    [Collection("Client collection")]
    public class CreateStream
    {
        private ClientFixture Fixture { get; set; }

        public CreateStream(ClientFixture c)
        {
            Fixture = c;
        }


        [Fact]
        public async Task TestCreateNewStreamAsync()
        {
            var streamName = Guid.NewGuid().ToString();
            await Fixture.Client.CreateStream(streamName, "test", Fixture.TimeoutToken);
            return;
        }

        [Fact]
        public async Task TestCreateStreamWithSameNameAsync()
        {
            var streamName = Guid.NewGuid().ToString();
            await Fixture.Client.CreateStream(streamName, "test.samename.async.first");
            await Assert.ThrowsAsync<StreamAlreadyExistsException>(
                async () => await Fixture.Client.CreateStream(streamName, "test.samename.async.second")
            );
            return;
        }

        [Fact]
        public async Task TestStreamExists()
        {
            var streamName = Guid.NewGuid().ToString();
            var preCreateResult = await Fixture.Client.StreamExists(streamName);
            await Fixture.Client.CreateStream(streamName, "test");
            var postCreateResult = await Fixture.Client.StreamExists(streamName);

            Assert.False(preCreateResult);
            Assert.True(postCreateResult);
            return;
        }
    }
}
