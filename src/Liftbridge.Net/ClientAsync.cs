using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


using AckPolicy = Proto.AckPolicy;

namespace Liftbridge.Net
{
    public class LiftbridgeException : Exception
    {
        public LiftbridgeException() { }
        public LiftbridgeException(string message) : base(message) { }
        public LiftbridgeException(string message, Exception inner) : base(message, inner) { }
    }
    public class ConnectionErrorException : LiftbridgeException
    {
        public ConnectionErrorException() { }
        public ConnectionErrorException(string message) : base(message) { }
        public ConnectionErrorException(string message, Exception inner) : base(message, inner) { }
    }
    public class StreamNotExistsException : LiftbridgeException
    {
        public StreamNotExistsException() { }
        public StreamNotExistsException(string message) : base(message) { }
        public StreamNotExistsException(string message, Exception inner) : base(message, inner) { }
    }
    public class StreamAlreadyExistsException : LiftbridgeException
    {
        public StreamAlreadyExistsException() { }
        public StreamAlreadyExistsException(string message) : base(message) { }
        public StreamAlreadyExistsException(string message, Exception inner) : base(message, inner) { }
    }
    public class BrokerNotFoundException : LiftbridgeException
    {
        public BrokerNotFoundException() { }
        public BrokerNotFoundException(string message) : base(message) { }
        public BrokerNotFoundException(string message, Exception inner) : base(message, inner) { }
    }
    public class PartitionPausedException : LiftbridgeException
    {
        public PartitionPausedException() { }
        public PartitionPausedException(string message) : base(message) { }
        public PartitionPausedException(string message, Exception inner) : base(message, inner) { }
    }
    public class StreamDeletedException : LiftbridgeException
    {
        public StreamDeletedException() { }
        public StreamDeletedException(string message) : base(message) { }
        public StreamDeletedException(string message, Exception inner) : base(message, inner) { }
    }
    public class StreamPausedException : LiftbridgeException
    {
        public StreamPausedException() { }
        public StreamPausedException(string message) : base(message) { }
        public StreamPausedException(string message, Exception inner) : base(message, inner) { }
    }
    public class EndOfReadonlyException : LiftbridgeException
    {
        public EndOfReadonlyException() { }
        public EndOfReadonlyException(string message) : base(message) { }
        public EndOfReadonlyException(string message, Exception inner) : base(message, inner) { }
    }
    public class AckTimeoutException : LiftbridgeException
    {
        public AckTimeoutException() { }
        public AckTimeoutException(string message) : base(message) { }
        public AckTimeoutException(string message, Exception inner) : base(message, inner) { }
    }

    public delegate Task MessageHandler(Proto.Message message);
    public delegate Task AckHandler(Proto.PublishResponse message);

    internal record AckContext
    {
        public AckHandler Handler { get; init; }
        public System.Timers.Timer Timer { get; init; }
    }

    public class ClientOptions
    {
        public IEnumerable<BrokerAddress> Brokers { get; init; }
        public string TLSCert { get; init; }
        public TimeSpan ResubscribeWaitTime { get; init; }
        public TimeSpan AckWaitTime { get; init; }
    }

    public class ClientAsync
    {
        const int RPCResiliencyTryCount = 10;
        const int SubscriptionResiliencyTryCount = 5;

        private Brokers Brokers;
        private MetadataCache metadata;

        private ImmutableDictionary<string, AckContext> AckContexts;

        private ClientOptions options;

        public ClientAsync(ClientOptions opts)
        {
            options = opts;
            metadata = new MetadataCache();
            Brokers = new Brokers(opts.Brokers, PublishResponseReceived);

            AckContexts = ImmutableDictionary<string, AckContext>.Empty;
        }

        private async Task<T> DoResilientRPC<T>(Func<Proto.API.APIClient, Task<T>> func)
        {
            for (var i = 0; i < RPCResiliencyTryCount; i++)
            {
                var broker = Brokers.GetRandom();
                try
                {
                    return await func(broker.Client);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.Unavailable)
                    {
                        await Task.Delay(100);
                        await UpdateMetadataCache();
                        continue;
                    }
                    throw;
                }
            }
            throw new ConnectionErrorException();
        }

        private async Task<T> DoResilientLeaderRPC<T>(string stream, int partition, Func<Proto.API.APIClient, Task<T>> func)
        {
            await Task.Delay(0);
            return default(T);
        }

        public async Task<Metadata> FetchMetadata()
        {
            return await DoResilientRPC(async client =>
            {
                var meta = await client.FetchMetadataAsync(new Proto.FetchMetadataRequest { }, deadline: DateTime.UtcNow.AddSeconds(10));
                var brokers = meta.Brokers
                        .Select(broker => BrokerInfo.FromProto(broker))
                        .ToImmutableHashSet();
                var streams = meta.Metadata
                    .Select(stream => new KeyValuePair<string, StreamInfo>(stream.Name, StreamInfo.FromProto(stream)))
                    .ToImmutableDictionary();
                return new Metadata
                {
                    LastUpdated = DateTime.UtcNow,
                    Brokers = brokers,
                    Streams = streams,
                };
            });
        }

        private async Task UpdateMetadataCache()
        {
            await metadata.Update(async () =>
            {
                return await FetchMetadata();
            });
            await Brokers.Update(metadata.GetAddresses());
        }

        public async Task<PartitionInfo> FetchPartitionMetadata(string stream, int partition)
        {
            return await DoResilientLeaderRPC(stream, partition, async client =>
            {
                var request = new Proto.FetchPartitionMetadataRequest { Stream = stream, Partition = partition };
                var result = await client.FetchPartitionMetadataAsync(request);

                var partitionMeta = result.Metadata;
                var leader = partitionMeta.Leader;
                var replicas = partitionMeta.Replicas.ToList();
                var isrs = partitionMeta.Isr.ToList();

                var replicaIds = replicas.Select(replica => metadata.GetBroker(replica).Id).ToImmutableHashSet();
                var isrIds = isrs.Select(isr => metadata.GetBroker(isr).Id).ToImmutableHashSet();

                var leaderInfo = metadata.GetBroker(leader);

                return new PartitionInfo
                {
                    Id = partitionMeta.Id,
                    Leader = leaderInfo.Id,
                    Replicas = replicaIds,
                    ISR = isrIds,
                    HighWatermark = partitionMeta.HighWatermark,
                    NewestOffset = partitionMeta.NewestOffset,
                    Paused = partitionMeta.Paused,
                    MessagesReceivedTimestamps = PartitionEventTimestamps.FromProto(partitionMeta.MessagesReceivedTimestamps),
                    PausedTimestamps = PartitionEventTimestamps.FromProto(partitionMeta.PauseTimestamps),
                    ReadonlyTimestamps = PartitionEventTimestamps.FromProto(partitionMeta.ReadonlyTimestamps),
                };
            });
        }

        public Task CreateStream(string name, string subject)
        {
            var opts = new StreamOptions();
            return CreateStream(name, subject, opts);
        }

        public async Task CreateStream(string name, string subject, StreamOptions streamOptions)
        {
            var request = streamOptions.Request();
            request.Name = name;
            request.Subject = subject;

            await DoResilientRPC(async client =>
            {
                try
                {
                    return await client.CreateStreamAsync(request);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.AlreadyExists)
                    {
                        throw new StreamAlreadyExistsException();
                    }
                    throw;
                }
            });
        }

        public async Task DeleteStream(string name)
        {
            var request = new Proto.DeleteStreamRequest { Name = name };
            await DoResilientRPC(async client =>
            {
                try
                {
                    return await client.DeleteStreamAsync(request);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.NotFound)
                    {
                        throw new StreamNotExistsException();
                    }
                    throw;
                }
            });
        }

        public async Task SetStreamReadonly(string name, IEnumerable<int> partitions = null, bool isReadOnly = true)
        {
            var request = new Proto.SetStreamReadonlyRequest { Name = name, Readonly = isReadOnly };
            if (partitions != null)
            {
                request.Partitions.AddRange(partitions);
            }
            await DoResilientRPC(async client =>
            {
                try
                {
                    return await client.SetStreamReadonlyAsync(request);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.NotFound)
                    {
                        throw new StreamNotExistsException();
                    }
                    throw;
                }
            });
        }

        public async Task PauseStream(string name, IEnumerable<int> partitions = null, bool resumeAll = false)
        {
            var request = new Proto.PauseStreamRequest { Name = name, ResumeAll = resumeAll };
            if (partitions != null)
            {
                request.Partitions.AddRange(partitions);
            }
            await DoResilientRPC(async client =>
            {
                try
                {
                    return await client.PauseStreamAsync(request);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.NotFound)
                    {
                        throw new StreamNotExistsException();
                    }
                    throw;
                }
            });
        }

        public async Task<Proto.Ack> Publish(string stream, byte[] value, MessageOptions opts)
        {
            opts = opts with { ExpectedOffset = 1 };

            if (opts.AckPolicy == AckPolicy.None)
            {
                // Fire and forget
                _ = PublishAsync(stream, value, null, opts, null);
                return null;
            }

            var ackOnce = new WriteOnceBlock<Proto.Ack>(null);
            // Publish and wait for ack
            await PublishAsync(stream, value, async (msg) =>
            {
                // If the message is null, the ack waiting timed out.
                if (msg is null)
                {
                    await ackOnce.SendAsync(null);
                }
                else
                {
                    await ackOnce.SendAsync(msg.Ack);
                }
            }, opts, null);
            var res = await ackOnce.ReceiveAsync();
            if(res is null)
            {
                throw new TimeoutException("Ack awaiting timed out.");
            }
            return res;
        }

        public Task PublishAsync(string stream, byte[] value, AckHandler ackHandler, DateTime? deadline)
        {
            var opts = new MessageOptions { };
            return PublishAsync(stream, value, ackHandler, opts, deadline);
        }

        public async Task PublishAsync(string stream, byte[] value, AckHandler ackHandler, MessageOptions opts, DateTime? deadline)
        {
            if (opts.CorrelationId == string.Empty)
            {
                opts = opts with { CorrelationId = Guid.NewGuid().ToString() };
            }

            int partition = 0;
            if (opts.Partition.HasValue)
            {
                partition = opts.Partition.Value;
            }
            else if (opts.Partitioner is not null)
            {
                if (!metadata.HasStreamInfo(stream))
                {
                    throw new StreamNotExistsException("No metadata for stream");
                }
                partition = opts.Partitioner.Partition(stream, opts.Key, value, metadata);
            }

            var key = opts.Key == null ? Google.Protobuf.ByteString.Empty : Google.Protobuf.ByteString.CopyFrom(opts.Key);
            var valueBytes = value == null ? Google.Protobuf.ByteString.Empty : Google.Protobuf.ByteString.CopyFrom(value);

            var request = new Proto.PublishRequest
            {
                Stream = stream,
                Partition = partition,
                Key = key,
                Value = valueBytes,
                AckInbox = opts.AckInbox,
                CorrelationId = opts.CorrelationId,
                AckPolicy = opts.AckPolicy,
                ExpectedOffset = opts.ExpectedOffset,
            };

            if (ackHandler is not null)
            {
                var timeout = options.AckWaitTime;
                if (deadline.HasValue && deadline.Value > DateTime.UtcNow)
                {
                    timeout = deadline.Value.Subtract(DateTime.UtcNow);
                }

                var ackCtx = new AckContext
                {
                    Handler = ackHandler,
                    Timer = new System.Timers.Timer(timeout.TotalMilliseconds),
                };
                ackCtx.Timer.Elapsed += async (sender, e) =>
                {
                    var ctx = RemoveAckContext(request.CorrelationId);
                    if (ctx is not null)
                    {
                        await ctx.Handler(null);
                    }
                };
                lock (AckContexts)
                {
                    AckContexts = AckContexts.Add(request.CorrelationId, ackCtx);
                    ackCtx.Timer.Start();
                }
            }

            var broker = Brokers.GetFromStream(stream, partition);
            await broker.Stream.RequestStream.WriteAsync(request);
        }

        private Task PublishResponseReceived(Proto.PublishResponse message)
        {
            string correlationId = null;
            if (message.CorrelationId is not null)
            {
                correlationId = message.CorrelationId;
            }
            else if (message.Ack.CorrelationId is not null)
            {
                correlationId = message.Ack.CorrelationId;
            }

            if (correlationId is not null)
            {
                var context = RemoveAckContext(correlationId);
                return context.Handler(message);
            }
            return null;
        }

        private AckContext RemoveAckContext(string correlationID)
        {
            lock (AckContexts)
            {
                var ctx = AckContexts[correlationID];
                AckContexts = AckContexts.Remove(correlationID);
                ctx.Timer.Stop();
                ctx.Timer.Dispose();
                return ctx;
            }
        }

        public async Task Subscribe(string stream, SubscriptionOptions opts, MessageHandler messageHandler)
        {
            for (var i = 1; i <= SubscriptionResiliencyTryCount; i++)
            {
                try
                {
                    var address = metadata.GetAddress(stream, opts.Partition, opts.ReadIsrReplica);
                    var broker = Brokers.GetFromAddress(address);

                    var request = new Proto.SubscribeRequest
                    {
                        Partition = opts.Partition,
                        ReadISRReplica = opts.ReadIsrReplica,
                    };
                    using var subscription = broker.Client.Subscribe(request);
                    try
                    {
                        while (await subscription.ResponseStream.MoveNext(default))
                        {
                            var message = subscription.ResponseStream.Current;
                            await messageHandler(message);
                        }
                    }
                    catch (Grpc.Core.RpcException ex)
                    {
                        switch (ex.StatusCode)
                        {
                            case Grpc.Core.StatusCode.Cancelled:
                                return;
                            case Grpc.Core.StatusCode.NotFound:
                                throw new StreamDeletedException();
                            case Grpc.Core.StatusCode.FailedPrecondition:
                                throw new StreamPausedException();
                            case Grpc.Core.StatusCode.ResourceExhausted:
                                // Indicates the end of a readonly partition has been reached.
                                throw new EndOfReadonlyException();
                            case Grpc.Core.StatusCode.Unavailable:
                                await Task.Delay(50);
                                await UpdateMetadataCache();
                                continue;
                        }
                        throw;
                    }
                }
                catch (BrokerNotFoundException)
                {
                    // Re-throws when the last try failed
                    if (i == SubscriptionResiliencyTryCount)
                    {
                        throw;
                    }
                    await Task.Delay(50);
                    await UpdateMetadataCache();
                }
            }
        }
    }

    public record SubscriptionOptions
    {
        public int Partition { get; init; }
        public bool ReadIsrReplica { get; init; }
    }

    public record MessageOptions
    {
        public byte[] Key { get; init; }
        public string AckInbox { get; init; } = String.Empty;
        public string CorrelationId { get; init; } = String.Empty;
        public AckPolicy AckPolicy { get; init; }
        public ImmutableDictionary<string, byte[]> Headers { get; init; }
        public IPartitioner Partitioner { get; init; }
        public int? Partition { get; init; }
        public long ExpectedOffset { get; init; }
    }
}
