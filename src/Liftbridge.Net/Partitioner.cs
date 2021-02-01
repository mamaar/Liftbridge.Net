using Crc32C;
using System;
using System.Collections.Generic;
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
            if (key == null)
            {
                key = Encoding.ASCII.GetBytes("");
            }

            var partitionsCount = metadata.StreamPartitionCount(stream);
            if(partitionsCount == 0)
            {
                return partitionsCount;
            }
            var hash = Crc32CAlgorithm.Compute(key.ToArray());
            return hash % partitionsCount;
        }
    }

    public class PartitionByRoundRobin : IPartitioner
    {
        public override long Partition(string stream, byte[] key, byte[] value, Metadata metadata)
        {
            throw new NotImplementedException();
        }
    }
}
