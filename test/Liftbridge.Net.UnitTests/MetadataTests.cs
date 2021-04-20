using Xunit;

namespace Liftbridge.Net.Tests
{
    public class MetadataTests
    {
        [Fact]
        public void TestGetActualBroker()
        {
            var metadata = new Metadata { };
            var b = new BrokerInfo { Id = "1", Host = "localhost", Port = 9292, };
            metadata = metadata with { Brokers = metadata.Brokers.Add(b.Id, b) };

            var broker = metadata.GetBroker("1");
            Assert.Equal(b, broker);
        }

        [Fact]
        public void TestDeserializeStreamInfoWithoutPartitions()
        {
            var protoStreamInfo = new Proto.StreamMetadata
            {
                Name = "test-stream",
                Subject = "test",
                CreationTimestamp = 1618912800_000000, // The Liftbridge server uses nanoseconds
                Error = Proto.StreamMetadata.Types.Error.Ok,
            };

            var streamInfo = StreamInfo.FromProto(protoStreamInfo);
            Assert.Equal(protoStreamInfo.Name, streamInfo.Name);
            Assert.Equal(protoStreamInfo.Subject, streamInfo.Subject);
            Assert.Equal(protoStreamInfo.CreationTimestamp, streamInfo.CreationTimestamp.ToUnixTimeMilliseconds() * 1_000_000);
        }
    }
}
