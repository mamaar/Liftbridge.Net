using System;

namespace Liftbridge.Net
{
    public class StreamOptions
    {

        public string Group = "";
        public int ReplicationFactor = 1;
        public int Partitions = 1;
        public long? RetentionMaxBytes;
        public long? RetentionMaxMessages;
        public TimeSpan? RetentionMaxAge;
        public TimeSpan? CleanerInterval;
        public long? SegmentMaxBytes;
        public TimeSpan? SegmentMaxAge;
        public int? CompactMaxGoroutines;
        public bool? CompactEnabled;
        public TimeSpan? AutoPauseTime;
        public bool? AutoPauseDisableIfSubscribers;
        public int? MinISR;
        public bool? OptimisticConcurrencyControl;

        public Proto.CreateStreamRequest Request()
        {
            var request = new Proto.CreateStreamRequest
            {
                Group = Group,
                ReplicationFactor = ReplicationFactor,
                Partitions = Partitions,
            };
            if (RetentionMaxBytes != null)
            {
                request.RetentionMaxBytes = new Proto.NullableInt64 { Value = RetentionMaxBytes.Value };
            }
            if (RetentionMaxMessages != null)
            {
                request.RetentionMaxMessages = new Proto.NullableInt64 { Value = RetentionMaxMessages.Value };
            }
            if (RetentionMaxAge != null)
            {
                request.RetentionMaxAge = new Proto.NullableInt64 { Value = (long)RetentionMaxAge.Value.TotalMilliseconds };
            }
            if (CleanerInterval != null)
            {
                request.CleanerInterval = new Proto.NullableInt64 { Value = (long)CleanerInterval.Value.TotalMilliseconds };
            }
            if (SegmentMaxBytes != null)
            {
                request.SegmentMaxBytes = new Proto.NullableInt64 { Value = SegmentMaxBytes.Value };
            }
            if (SegmentMaxAge != null)
            {
                request.SegmentMaxAge = new Proto.NullableInt64 { Value = (long)SegmentMaxAge.Value.TotalMilliseconds };
            }
            if (CompactMaxGoroutines != null)
            {
                request.CompactMaxGoroutines = new Proto.NullableInt32 { Value = CompactMaxGoroutines.Value };
            }
            if (CompactEnabled != null)
            {
                request.CompactEnabled = new Proto.NullableBool { Value = CompactEnabled.Value };
            }
            if (AutoPauseTime != null)
            {
                request.AutoPauseTime = new Proto.NullableInt64 { Value = (long)AutoPauseTime.Value.TotalMilliseconds };
            }
            if (AutoPauseDisableIfSubscribers != null)
            {
                request.AutoPauseDisableIfSubscribers = new Proto.NullableBool { Value = AutoPauseDisableIfSubscribers.Value };
            }
            if (MinISR != null)
            {
                request.MinIsr = new Proto.NullableInt32 { Value = MinISR.Value };
            }
            if (OptimisticConcurrencyControl != null)
            {
                request.OptimisticConcurrencyControl = new Proto.NullableBool { Value = OptimisticConcurrencyControl.Value };
            }

            return request;
        }
    }
}
