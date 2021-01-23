using System;
using System.Collections.Immutable;
using System.Collections.Generic;
using System.Linq;

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
        public ImmutableList<string> Replicas { get; init; }
        public ImmutableList<string> ISR { get; init; }
        public long HighWatermark { get; init; }
        public long NewestOffset { get; init; }
        public bool Paused { get; init; }
        public bool Readonly { get; init; }
        public PartitionEventTimestamps MessagesReceivedTimestamps { get; init; }
        public PartitionEventTimestamps PausedTimestamps { get; init; }
        public PartitionEventTimestamps ReadonlyTimestamps { get; init; }

        public static PartitionInfo FromProto(Proto.PartitionMetadata proto)
        {
            return new PartitionInfo {
                Id = proto.Id,
                Leader = proto.Leader,
                Replicas = ImmutableList<string>.Empty.AddRange(proto.Replicas),
                ISR = ImmutableList<string>.Empty.AddRange(proto.Isr),
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

        public static StreamInfo FromProto(Proto.StreamMetadata proto) {
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
    }

    public record Metadata {
        public DateTime LastUpdated { get; init; } = DateTime.MinValue;
        public ImmutableHashSet<BrokerInfo> Brokers { get; init; } = ImmutableHashSet<BrokerInfo>.Empty;
        public ImmutableDictionary<string, StreamInfo> Streams { get; init; } = ImmutableDictionary<string, StreamInfo>.Empty;

        public ImmutableHashSet<BrokerAddress> BootstrapAddresses { get; init; } = ImmutableHashSet<BrokerAddress>.Empty;

        public BrokerInfo GetBroker(string brokerId)
        {
            try
            {
                return Brokers.Single(broker => broker.Id == brokerId);
            }
            catch(ArgumentNullException)
            {
                throw new BrokerNotFoundException();
            }
        }

        public BrokerAddress GetAddress(string streamName, int partitionId, bool isISRReplica)
        {
            var stream = Streams[streamName];
            var partitionInfo = stream.GetPartition(partitionId);
            var leader = GetBroker(partitionInfo.Leader);

            return leader.GetAddress();
        }

        public ImmutableList<BrokerAddress> GetAddresses()
        {
            return ImmutableList<BrokerAddress>.Empty
                .AddRange(Brokers.Select((broker, _) => broker.GetAddress()))
                .AddRange(BootstrapAddresses);
        }
    }
}
