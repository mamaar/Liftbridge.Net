using System;

namespace Liftbridge.Net
{
    public record SubscriptionOptions
    {
        public Proto.StartPosition StartPosition { get; init; }
        public long StartOffset { get; init; }
        public DateTimeOffset StartTimestamp { get; init; }
        public Proto.StopPosition StopPosition { get; init; }
        public long StopOffset { get; init; }
        public DateTimeOffset StopTimestamp { get; init; }
        public int Partition { get; init; }
        public bool ReadIsrReplica { get; init; }
        public bool Resume { get; init; }
    }
}
