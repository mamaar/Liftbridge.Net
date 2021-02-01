using Crc32C;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;

namespace Liftbridge.Net
{
    public abstract class IPartitioner
    {
        public abstract long Partition(string stream, byte[] key, byte[] value, Metadata metadata);

        public long Partition(string stream, string key, string value, Metadata metadata) => Partition(stream, Encoding.ASCII.GetBytes(key), Encoding.ASCII.GetBytes(value), metadata);
    }

    public class PartitionByKey : IPartitioner
    {
        public override long Partition(string stream, byte[] key, byte[] value, Metadata metadata)
        {
            var partitionsCount = metadata.StreamPartitionCount(stream);
            if (partitionsCount == 0)
            {
                return partitionsCount;
            }

            if (key == null)
            {
                key = Encoding.ASCII.GetBytes("");
            }

            var hash = Crc32CAlgorithm.Compute(key.ToArray());
            return hash % partitionsCount;
        }
    }

    public class PartitionByRoundRobin : IPartitioner
    {
        private ImmutableDictionary<string, int> counter;
        public PartitionByRoundRobin()
        {
            counter = ImmutableDictionary<string, int>.Empty;
        }

        public override long Partition(string stream, byte[] key, byte[] value, Metadata metadata)
        {
            var partitionsCount = metadata.StreamPartitionCount(stream);
            if (partitionsCount == 0)
            {
                return 0;
            }

            lock (counter)
            {
                var currentCount = counter.GetValueOrDefault(stream, 0);
                counter = counter.SetItem(stream, currentCount + 1);
                return counter[stream] % partitionsCount;
            }
        }
    }
}
