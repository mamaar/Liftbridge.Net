using Google.Protobuf;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Liftbridge.Net
{
    public record MessageOptions
    {
        public byte[] Key { get; init; }
        public string AckInbox { get; init; } = String.Empty;
        public string CorrelationId { get; init; } = String.Empty;
        public Proto.AckPolicy AckPolicy { get; init; }
        public ImmutableDictionary<string, byte[]> Headers { get; init; }
        public IPartitioner Partitioner { get; init; }
        public int Partition { get; init; } = 0;
        public long ExpectedOffset { get; init; }
    }

    /// <summary>
    /// Represents a message consumed from Liftbridge.
    /// </summary>
    public record Message
    {
        public long Offset { get; init; }
        public byte[] Key { get; init; }
        public byte[] Value { get; init; }
        public int Partition { get; init; }
        public DateTimeOffset Timestamp { get; init; }
        public string Stream { get; init; }
        public string Subject { get; init; }
        public string ReplySubject { get; init; }
        public ImmutableDictionary<string, byte[]> Headers { get; init; }

        public static Message FromProto(Proto.Message proto)
        {
            return new Message
            {
                Offset = proto.Offset,
                Key = proto.Key.ToByteArray(),
                Value = proto.Value.ToByteArray(),
                Partition = proto.Partition,
                Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(proto.Timestamp / 1_000_000),
                Stream = proto.Stream,
                Subject = proto.Subject,
                ReplySubject = proto.ReplySubject,
                Headers = proto.Headers
                    .Select((pair, _) => KeyValuePair.Create(pair.Key, pair.Value.ToByteArray()))
                    .ToImmutableDictionary(),
            };
        }

        public Proto.Message ToProto()
        {
            var message = new Proto.Message
            {
                Offset = Offset,
                Key = ByteString.CopyFrom(Key),
                Value = ByteString.CopyFrom(Value),
                Partition = Partition,
                Timestamp = Timestamp.ToUnixTimeMilliseconds() * 1_000_000,
                Stream = Stream,
                Subject = Subject,
                ReplySubject = ReplySubject,
            };
            foreach (var header in Headers)
            {
                message.Headers.Add(header.Key, ByteString.CopyFrom(header.Value));
            }

            return message;
        }
    }
}
