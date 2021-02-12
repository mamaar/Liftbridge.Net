using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;

namespace Liftbridge.Net
{
    public abstract class IPartitioner
    {
        public abstract int Partition(string stream, byte[] key, byte[] value, IMetadata metadata);

        public int Partition(string stream, string key, string value, Metadata metadata) => Partition(stream, Encoding.ASCII.GetBytes(key), Encoding.ASCII.GetBytes(value), metadata);
    }

    public class PartitionByKey : IPartitioner
    {
        public override int Partition(string stream, byte[] key, byte[] value, IMetadata metadata)
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

            var hash = Dexiom.QuickCrc32.QuickCrc32.Compute(key) % partitionsCount;
            return (int)hash;
        }
    }

    public class PartitionByRoundRobin : IPartitioner
    {
        private ImmutableDictionary<string, int> counter;
        public PartitionByRoundRobin()
        {
            counter = ImmutableDictionary<string, int>.Empty;
        }

        public override int Partition(string stream, byte[] key, byte[] value, IMetadata metadata)
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
