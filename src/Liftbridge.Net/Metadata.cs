using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Liftbridge.Net
{

    public record PartitionEventTimestamps
    {
        public DateTime FirstTime;
        public DateTime LatestTime;

        public static PartitionEventTimestamps FromProto(Proto.PartitionEventTimestamps proto)
        {
            return new PartitionEventTimestamps
            {
                FirstTime = DateTime.UnixEpoch.Add(TimeSpan.FromMilliseconds(proto.FirstTimestamp / 1_000_000)),
                LatestTime = DateTime.UnixEpoch.Add(TimeSpan.FromMilliseconds(proto.LatestTimestamp / 1_000_000)),
            };
        }
    }

    public record PartitionInfo
    {
        public int Id { get; init; }
        public string Leader { get; init; }
        public ImmutableHashSet<string> Replicas { get; init; }
        public ImmutableHashSet<string> ISR { get; init; }
        public long HighWatermark { get; init; }
        public long NewestOffset { get; init; }
        public bool Paused { get; init; }
        public bool Readonly { get; init; }
        public PartitionEventTimestamps MessagesReceivedTimestamps { get; init; }
        public PartitionEventTimestamps PausedTimestamps { get; init; }
        public PartitionEventTimestamps ReadonlyTimestamps { get; init; }

        public static PartitionInfo FromProto(Proto.PartitionMetadata proto)
        {
            return new PartitionInfo
            {
                Id = proto.Id,
                Leader = proto.Leader,
                Replicas = proto.Replicas.ToImmutableHashSet(),
                ISR = proto.Isr.ToImmutableHashSet(),
                HighWatermark = proto.HighWatermark,
                NewestOffset = proto.NewestOffset,
                Paused = proto.Paused,
                Readonly = proto.Readonly,
                MessagesReceivedTimestamps = PartitionEventTimestamps.FromProto(proto.MessagesReceivedTimestamps),
                PausedTimestamps = PartitionEventTimestamps.FromProto(proto.PauseTimestamps),
                ReadonlyTimestamps = PartitionEventTimestamps.FromProto(proto.ReadonlyTimestamps),
            };
        }
    }

    public record StreamInfo
    {
        public string Name { get; init; }
        public string Subject { get; init; }
        public DateTime CreationTimestamp { get; init; }
        public ImmutableDictionary<int, PartitionInfo> Partitions { get; init; }

        public PartitionInfo GetPartition(int partitionId)
        {
            return Partitions[partitionId];
        }

        public static StreamInfo FromProto(Proto.StreamMetadata proto)
        {
            return new StreamInfo
            {
                CreationTimestamp = DateTime.UnixEpoch.Add(TimeSpan.FromMilliseconds(proto.CreationTimestamp / 1_000_000)),
                Name = proto.Name,
                Partitions = ImmutableDictionary<int, PartitionInfo>.Empty
                    .AddRange(proto.Partitions.Select((partition, _) =>
                        new KeyValuePair<int, PartitionInfo>(partition.Key, PartitionInfo.FromProto(partition.Value))
                    )),
            };
        }

        internal bool TryGetPartition(int partitionId, out PartitionInfo partitionInfo)
        {
            return Partitions.TryGetValue(partitionId, out partitionInfo);
        }
    }


    public record BrokerAddress
    {
        public string Host { get; init; }
        public int Port { get; init; }

        override public string ToString()
        {
            return $"{Host}:{Port}";
        }
    }

    public record BrokerInfo
    {
        public string Id { get; init; }
        public string Host { get; init; }
        public int Port { get; init; }

        public BrokerAddress Address
        {
            get
            {
                return new BrokerAddress { Host = Host, Port = Port };
            }
        }

        public static BrokerInfo FromProto(Proto.Broker broker)
        {
            return new BrokerInfo
            {
                Id = broker.Id,
                Host = broker.Host,
                Port = broker.Port,
            };
        }
    }

    public interface IMetadata
    {
        bool HasBrokers();
        BrokerInfo GetBroker(string brokerId);
        BrokerInfo GetBroker(string streamName, int partitionId, bool isISRReplica);
        BrokerInfo GetLeader(string stream, int partition);
        bool TryGetBroker(string brokerId, out BrokerInfo broker);
        bool TryGetBroker(string streamName, int partitionId, bool isISRReplica, out BrokerInfo broker);
        bool TryGetLeader(string stream, int partition, out BrokerInfo broker);
        ImmutableList<BrokerAddress> GetAddresses();
        StreamInfo GetStreamInfo(string stream);
        bool HasStreamInfo(string stream);
        int StreamPartitionCount(string stream);
    }

    public record Metadata : IMetadata
    {
        public DateTime LastUpdated { get; init; } = DateTime.UtcNow;
        public ImmutableDictionary<string, BrokerInfo> Brokers { get; init; } = ImmutableDictionary<string, BrokerInfo>.Empty;
        public ImmutableDictionary<string, StreamInfo> Streams { get; init; } = ImmutableDictionary<string, StreamInfo>.Empty;

        public BrokerInfo GetBroker(string brokerId)
        {
            if (!TryGetBroker(brokerId, out BrokerInfo broker))
            {
                throw new BrokerNotFoundException();
            }
            return broker;
        }

        public BrokerInfo GetBroker(string streamName, int partitionId, bool isISRReplica)
        {
            if (!TryGetBroker(streamName, partitionId, isISRReplica, out BrokerInfo broker))
            {
                throw new StreamNotExistsException();
            }
            return broker;
        }

        public ImmutableList<BrokerAddress> GetAddresses()
        {
            return ImmutableList<BrokerAddress>.Empty
                .AddRange(Brokers.Values.Select((broker, _) => broker.Address));
        }

        public StreamInfo GetStreamInfo(string stream)
        {
            if (!Streams.ContainsKey(stream))
            {
                throw new StreamNotExistsException();
            }

            return Streams[stream];
        }

        public bool HasStreamInfo(string stream)
        {
            return Streams.ContainsKey(stream);
        }

        public int StreamPartitionCount(string stream)
        {
            try
            {
                return GetStreamInfo(stream).Partitions.Count;
            }
            catch (StreamNotExistsException)
            {
                return 0;
            }
        }

        public bool HasBrokers()
        {
            return !Brokers.IsEmpty;
        }

        public BrokerInfo GetLeader(string stream, int partition)
        {
            return GetBroker(stream, partition, false);
        }

        public bool TryGetBroker(string brokerId, out BrokerInfo broker)
        {
            return Brokers.TryGetValue(brokerId, out broker);
        }

        public bool TryGetBroker(string streamName, int partitionId, bool isISRReplica, out BrokerInfo broker)
        {
            if (!Streams.TryGetValue(streamName, out StreamInfo stream))
            {
                broker = null;
                return false;
            }

            if (!stream.TryGetPartition(partitionId, out PartitionInfo partitionInfo))
            {
                broker = null;
                return false;
            }

            if (isISRReplica)
            {
                var rand = new Random();
                var isrId = partitionInfo.ISR.ElementAt(rand.Next(partitionInfo.ISR.Count));
                return TryGetBroker(isrId, out broker);
            }
            return TryGetBroker(partitionInfo.Leader, out broker);
        }

        public bool TryGetLeader(string stream, int partition, out BrokerInfo broker)
        {
            return TryGetBroker(stream, partition, false, out broker);
        }
    }

    public class MetadataCache : IMetadata
    {
        private Metadata MetadataData = new Metadata { };
        private System.Threading.SemaphoreSlim Semaphore = new System.Threading.SemaphoreSlim(1);

        internal async Task Update(IEnumerable<string> streams, Func<Task<Metadata>> fetchHandler)
        {
            await Semaphore.WaitAsync();
            var newMetadata = await fetchHandler();
            // Updates all streams
            if (!streams.Any())
            {
                MetadataData = newMetadata;
            }
            // Updates only specified streams
            else
            {
                var updatedStreams = MetadataData.Streams;

                // Removes streams from the cache that are not found in the cluster
                foreach (var stream in streams.Where(s => !newMetadata.HasStreamInfo(s)))
                {
                    updatedStreams = updatedStreams.Remove(stream);
                }
                // Update the stream info for existing streams
                foreach (var stream in streams.Where(s => newMetadata.HasStreamInfo(s)))
                {
                    updatedStreams = updatedStreams.SetItem(stream, newMetadata.Streams[stream]);
                }
                MetadataData = newMetadata with { Streams = updatedStreams };
            }
            Semaphore.Release();
        }

        /// <summary>
        /// Removes the stream entry from the cache.
        /// </summary>
        public void RemoveStream(string stream)
        {
            Semaphore.Wait();
            MetadataData = MetadataData with { Streams = MetadataData.Streams.Remove(stream) };
            Semaphore.Release();
        }

        public ImmutableList<BrokerAddress> GetAddresses()
        {
            return ((IMetadata)MetadataData).GetAddresses();
        }

        public StreamInfo GetStreamInfo(string stream)
        {
            return ((IMetadata)MetadataData).GetStreamInfo(stream);
        }

        public bool HasStreamInfo(string stream)
        {
            return ((IMetadata)MetadataData).HasStreamInfo(stream);
        }

        public int StreamPartitionCount(string stream)
        {
            return ((IMetadata)MetadataData).StreamPartitionCount(stream);
        }

        public BrokerInfo GetBroker(string brokerId)
        {
            return ((IMetadata)MetadataData).GetBroker(brokerId);
        }

        public bool TryGetBroker(string brokerId, out BrokerInfo broker)
        {
            return ((IMetadata)MetadataData).TryGetBroker(brokerId, out broker);
        }

        public BrokerInfo GetBroker(string streamName, int partitionId, bool isISRReplica)
        {
            return ((IMetadata)MetadataData).GetBroker(streamName, partitionId, isISRReplica);
        }

        public bool HasBrokers()
        {
            return ((IMetadata)MetadataData).HasBrokers();
        }

        public BrokerInfo GetLeader(string stream, int partition)
        {
            return ((IMetadata)MetadataData).GetLeader(stream, partition);
        }

        public bool TryGetBroker(string streamName, int partitionId, bool isISRReplica, out BrokerInfo broker)
        {
            return ((IMetadata)MetadataData).TryGetBroker(streamName, partitionId, isISRReplica, out broker);
        }

        public bool TryGetLeader(string stream, int partition, out BrokerInfo broker)
        {
            return ((IMetadata)MetadataData).TryGetLeader(stream, partition, out broker);
        }
    }
}
