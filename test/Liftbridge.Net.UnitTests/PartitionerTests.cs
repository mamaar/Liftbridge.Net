using Xunit;

namespace Liftbridge.Net.Tests
{
    public class PartitionerTests
    {
        [Fact]
        public void TestNoPartitionsIfStreamDoesNotExist()
        {
            var partitioner = new PartitionByKey();
            var partition = partitioner.Partition("test-stream", "bar", "baz", new Metadata());

            Assert.Equal(0, partition);
        }

        [Fact]
        public void TestPartitionByKey()
        {
            var partitioner = new PartitionByKey();
            var metadata = new Metadata
            {
                Streams = System.Collections.Immutable.ImmutableDictionary<string, StreamInfo>
                    .Empty
                    .Add("foo", new StreamInfo
                    {
                        Partitions = System.Collections.Immutable.ImmutableDictionary<int, PartitionInfo>
                        .Empty
                        .Add(0, new PartitionInfo())
                        .Add(1, new PartitionInfo())
                    }),
            };

            long partition;
            partition = partitioner.Partition("foo", "foobarbazqux", "1", metadata);
            Assert.Equal(0, partition);

            partition = partitioner.Partition("foo", "foobarbazqux", "2", metadata);
            Assert.Equal(0, partition);

            partition = partitioner.Partition("foo", "blar", "3", metadata);
            Assert.Equal(1, partition);

            partition = partitioner.Partition("foo", "blar", "4", metadata);
            Assert.Equal(1, partition);
        }
    }
}