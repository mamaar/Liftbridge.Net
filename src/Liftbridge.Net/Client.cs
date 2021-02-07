using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;


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

    public delegate Task MessageHandler(Proto.Message message);

    public class ClientOptions
    {
        public IEnumerable<BrokerAddress> Brokers { get; init; }
        public uint MaxConnsPerBroker { get; init; }
        public TimeSpan KeepAliveTime { get; init; }
        public string TLSCert { get; init; }
        public TimeSpan ResubscribeWaitTime { get; init; }
    }

    public class Client
    {
        const int RPCResiliencyTryCount = 10;
        const int SubscriptionResiliencyTryCount = 5;

        private Brokers Brokers;
        private Metadata metadata;

        private ClientOptions options;

        public Client(ClientOptions opts)
        {
            options = opts;
            metadata = new Metadata
            {
                Brokers = ImmutableHashSet<BrokerInfo>.Empty,
                BootstrapAddresses = options.Brokers.ToImmutableHashSet(),
            };

            Brokers = new Brokers(metadata.BootstrapAddresses);
        }

        private async Task<T> DoResilientRPCAsync<T>(Func<Proto.API.APIClient, Task<T>> func)
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
            return default(T);
        }

        public async Task<Metadata> FetchMetadataAsync()
        {
            return await DoResilientRPCAsync(async client =>
            {
                var meta = await client.FetchMetadataAsync(new Proto.FetchMetadataRequest { }, deadline: DateTime.UtcNow.AddSeconds(10));
                var brokers = meta.Brokers
                        .Select(broker => BrokerInfo.FromProto(broker))
                        .ToImmutableHashSet();
                var streams = meta.Metadata
                    .Select(stream => new KeyValuePair<string, StreamInfo>(stream.Name, StreamInfo.FromProto(stream)))
                    .ToImmutableDictionary();
                return metadata with
                {
                    LastUpdated = DateTime.UtcNow,
                    Brokers = brokers,
                    Streams = streams,
                };
            });
        }

        private async Task UpdateMetadataCache()
        {
            metadata = await FetchMetadataAsync();
            await Brokers.Update(metadata.GetAddresses());
        }

        public async Task<PartitionInfo> FetchPartitionMetadataAsync(string stream, int partition)
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

        public Task CreateStreamAsync(string name, string subject)
        {
            var opts = new StreamOptions();
            return CreateStreamAsync(name, subject, opts);
        }

        public async Task CreateStreamAsync(string name, string subject, StreamOptions streamOptions)
        {
            var request = streamOptions.Request();
            request.Name = name;
            request.Subject = subject;

            await DoResilientRPCAsync(async client =>
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

        public async Task DeleteStreamAsync(string name)
        {
            var request = new Proto.DeleteStreamRequest { Name = name };
            await DoResilientRPCAsync(async client =>
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

        public async Task SetStreamReadonlyAsync(string name, IEnumerable<int> partitions = null, bool isReadOnly = true)
        {
            var request = new Proto.SetStreamReadonlyRequest { Name = name, Readonly = isReadOnly };
            if (partitions != null)
            {
                request.Partitions.AddRange(partitions);
            }
            await DoResilientRPCAsync(async client =>
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

        public async Task PauseStreamAsync(string name, IEnumerable<int> partitions = null, bool resumeAll = false)
        {
            var request = new Proto.PauseStreamRequest { Name = name, ResumeAll = resumeAll };
            if (partitions != null)
            {
                request.Partitions.AddRange(partitions);
            }
            await DoResilientRPCAsync(async client =>
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

        public async Task PublishAsync(string stream, byte[] value, MessageOptions opts)
        {
            if (opts.CorrelationId == string.Empty)
            {
                opts = opts with { CorrelationId = Guid.NewGuid().ToString() };
            }

            int partition = 0;
            if (opts.Partition != null)
            {
                partition = opts.Partition.Value;
            }
            else if (opts.Partitioner != null)
            {
                if (!metadata.HasStreamInfo(stream))
                {
                    throw new StreamNotExistsException("No metadata for stream");
                }
                partition = opts.Partitioner.Partition(stream, opts.Key, value, metadata);
            }

            var request = new Proto.PublishRequest
            {
                Stream = stream,
                Partition = partition,
                Key = Google.Protobuf.ByteString.CopyFrom(opts.Key),
                Value = Google.Protobuf.ByteString.CopyFrom(value),
                AckInbox = opts.AckInbox,
                CorrelationId = opts.CorrelationId,
                AckPolicy = opts.AckPolicy,
                ExpectedOffset = opts.ExpectedOffset,
            };

            var broker = Brokers.GetFromStream(stream, partition);
            await broker.Stream.RequestStream.WriteAsync(request);
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
                                metadata = await FetchMetadataAsync();
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
        public string AckInbox { get; init; }
        public string CorrelationId { get; init; }
        public AckPolicy AckPolicy { get; init; }
        public ImmutableDictionary<string, byte[]> Headers { get; init; }
        public IPartitioner Partitioner { get; init; }
        public int? Partition { get; init; }
        public long ExpectedOffset { get; init; }
    }
}
