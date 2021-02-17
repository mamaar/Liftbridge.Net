using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;



namespace Liftbridge.Net
{
    using AckPolicy = Proto.AckPolicy;
    using StartPosition = Proto.StartPosition;
    using StopPosition = Proto.StopPosition;

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
    public class ReadOnlyException : LiftbridgeException
    {
        public ReadOnlyException() { }
        public ReadOnlyException(string message) : base(message) { }
        public ReadOnlyException(string message, Exception inner) : base(message, inner) { }
    }

    public delegate Task MessageHandler(Message message);
    public delegate Task AckHandler(Proto.PublishResponse message);

    internal record AckContext
    {
        public AckHandler Handler { get; init; }
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

        private Brokers Brokers { get; set; }
        private MetadataCache Metadata { get; set; }

        private ImmutableDictionary<string, AckContext> AckContexts;

        private ClientOptions options;

        public ClientAsync(ClientOptions opts)
        {
            options = opts;
            Metadata = new MetadataCache();
            Brokers = new Brokers(opts.Brokers, PublishResponseReceived);

            AckContexts = ImmutableDictionary<string, AckContext>.Empty;
        }

        private async Task<T> DoResilientRPC<T>(Func<Proto.API.APIClient, CancellationToken, Task<T>> func, CancellationToken cancellationToken = default)
        {
            var hasBrokers = Metadata.HasBrokers();
            if (!Metadata.HasBrokers())
            {
                await UpdateMetadataCache(cancellationToken);
            }
            for (var i = 0; i < RPCResiliencyTryCount; i++)
            {
                var broker = Brokers.GetRandom();
                try
                {
                    return await func(broker.Client, cancellationToken);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.Unavailable)
                    {
                        try
                        {
                            var cancelTokenSource = new CancellationTokenSource();
                            cancelTokenSource.CancelAfter(1000);
                            await UpdateMetadataCache(cancelTokenSource.Token);
                            continue;
                        }
                        catch (Grpc.Core.RpcException)
                        {
                        }
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

        public async Task<Metadata> FetchMetadata(CancellationToken cancellationToken = default)
        {
            foreach (var broker in Brokers)
            {
                var meta = await broker.Client.FetchMetadataAsync(new Proto.FetchMetadataRequest { }, cancellationToken: cancellationToken);
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
            }

            return new Metadata() { LastUpdated = DateTime.MinValue };
        }

        private async Task UpdateMetadataCache(CancellationToken cancellationToken = default)
        {
            await Metadata.Update(async () =>
            {
                return await FetchMetadata(cancellationToken);
            });
            await Brokers.Update(Metadata.GetAddresses());
        }

        public async Task<PartitionInfo> FetchPartitionMetadata(string stream, int partition, CancellationToken cancellationToken = default)
        {
            return await DoResilientLeaderRPC(stream, partition, async client =>
            {
                var request = new Proto.FetchPartitionMetadataRequest { Stream = stream, Partition = partition };
                var result = await client.FetchPartitionMetadataAsync(request, cancellationToken: cancellationToken);

                var partitionMeta = result.Metadata;
                var leader = partitionMeta.Leader;
                var replicas = partitionMeta.Replicas.ToList();
                var isrs = partitionMeta.Isr.ToList();

                var replicaIds = replicas.Select(replica => Metadata.GetBroker(replica).Id).ToImmutableHashSet();
                var isrIds = isrs.Select(isr => Metadata.GetBroker(isr).Id).ToImmutableHashSet();

                var leaderInfo = Metadata.GetBroker(leader);

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

        /// <summary>
        /// CreateStream creates a new stream attached to a NATS subject or set of NATS subjects.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="subject"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="StreamAlreadyExistsException">Thrown when a stream with the given name already exists.</exception>
        public Task CreateStream(string stream, string subject, CancellationToken cancellationToken = default)
        {
            var opts = new StreamOptions();
            return CreateStream(stream, subject, opts, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// CreateStream creates a new stream attached to a NATS subject or set of NATS subjects.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="subject"></param>
        /// <param name="streamOptions"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="StreamAlreadyExistsException">Thrown when a stream with the given name already exists.</exception>
        public async Task CreateStream(string stream, string subject, StreamOptions streamOptions, CancellationToken cancellationToken = default)
        {
            var request = streamOptions.Request();
            request.Name = stream;
            request.Subject = subject;

            await DoResilientRPC(async (client, cancelToken) =>
            {
                try
                {
                    return await client.CreateStreamAsync(request, cancellationToken: cancelToken);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.AlreadyExists)
                    {
                        throw new StreamAlreadyExistsException();
                    }
                    throw;
                }
            }, cancellationToken);
        }

        /// <summary>
        /// DeleteStream deletes a stream and all of its partitions. This will remove any data stored on disk for the stream and all of its partitions.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="StreamNotExistsException">Thrown when a stream with the given name does not exist.</exception>
        public async Task DeleteStream(string stream, CancellationToken cancellationToken = default)
        {
            var request = new Proto.DeleteStreamRequest { Name = stream };
            await DoResilientRPC(async (client, cancelToken) =>
            {
                try
                {
                    return await client.DeleteStreamAsync(request, cancellationToken: cancelToken);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.NotFound)
                    {
                        throw new StreamNotExistsException();
                    }
                    throw;
                }
            }, cancellationToken);
        }

        /// <summary>
        /// SetStreamReadonly sets some or all of a stream's partitions as readonly or readwrite.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="partitions"></param>
        /// <param name="isReadOnly">Whether the stream partitions should be readonly or not.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task SetStreamReadonly(string stream, IEnumerable<int> partitions = null, bool isReadOnly = true, CancellationToken cancellationToken = default)
        {
            var request = new Proto.SetStreamReadonlyRequest { Name = stream, Readonly = isReadOnly };
            if (partitions != null)
            {
                request.Partitions.AddRange(partitions);
            }
            await DoResilientRPC(async (client, cancelToken) =>
            {
                try
                {
                    return await client.SetStreamReadonlyAsync(request, cancellationToken: cancelToken);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.NotFound)
                    {
                        throw new StreamNotExistsException();
                    }
                    throw;
                }
            }, cancellationToken);
        }

        /// <summary>
        /// PauseStream pauses some or all of a stream's partitions.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="partitions"></param>
        /// <param name="resumeAll"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="StreamNotExistsException">Thrown when a stream with the given name does not exist.</exception>
        public async Task PauseStream(string stream, IEnumerable<int> partitions = null, bool resumeAll = false, CancellationToken cancellationToken = default)
        {
            var request = new Proto.PauseStreamRequest { Name = stream, ResumeAll = resumeAll };
            if (partitions != null)
            {
                request.Partitions.AddRange(partitions);
            }
            await DoResilientRPC(async (client, cancelToken) =>
            {
                try
                {
                    return await client.PauseStreamAsync(request, cancellationToken: cancelToken);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.NotFound)
                    {
                        throw new StreamNotExistsException();
                    }
                    throw;
                }
            }, cancellationToken);
        }

        /// <summary>
        /// Publish sends a message to a Liftbridge stream.
        /// Publish is a synchronous operation, meaning when it returns, the message has been successfully published.
        /// Publish can also be configured to block until a message acknowledgement (ack) is returned from the cluster. 
        /// This is useful for ensuring a message has been stored and replicated, guaranteeing at-least-once delivery. 
        /// The default ack policy is Leader, meaning the ack is sent once the partition leader has stored the message.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="value"></param>
        /// <param name="opts"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>Returns the ack, or null if AckPolicy is set to None.</returns>
        /// <exception cref="TimeoutException">Thrown when the ack was not returned in time.</exception>
        public async Task<Proto.Ack> Publish(string stream, byte[] value, MessageOptions opts, CancellationToken cancellationToken = default)
        {
            opts = opts with { ExpectedOffset = 1 };

            if (opts.AckPolicy == AckPolicy.None)
            {
                // Fire and forget
                _ = PublishAsync(stream, value, ackHandler: null, opts, cancellationToken: cancellationToken);
                return null;
            }

            var ackOnce = new WriteOnceBlock<Proto.Ack>(null);
            // Publish and wait for ack or timeout
            await PublishAsync(stream, value, async (msg) =>
            {
                // If the message is null, the ack waiting timed out.
                if (msg is null)
                {
                    await ackOnce.SendAsync(null, cancellationToken);
                }
                else
                {
                    await ackOnce.SendAsync(msg.Ack, cancellationToken);
                }
            }, opts, cancellationToken: cancellationToken);
            var res = await ackOnce.ReceiveAsync(cancellationToken);
            if (res is null)
            {
                throw new TimeoutException("Ack awaiting timed out.");
            }
            return res;
        }

        /// <summary>
        /// PublishAsync sends a message to a Liftbridge stream asynchronously. This is similar to <c>Publish</c>, but rather than waiting for the ack, it dispatches the ack with an ack handler callback.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="value"></param>
        /// <param name="ackHandler"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task PublishAsync(string stream, byte[] value, AckHandler ackHandler, CancellationToken cancellationToken = default)
        {
            var opts = new MessageOptions { };
            return PublishAsync(stream, value, ackHandler, opts, cancellationToken);
        }

        /// <summary>
        /// PublishAsync sends a message to a Liftbridge stream asynchronously. This is similar to <c>Publish</c>, but rather than waiting for the ack, it dispatches the ack with an ack handler callback.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="value"></param>
        /// <param name="ackHandler"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="ReadOnlyException">Thrown when the stream partition is set to readonly.</exception>
        public async Task PublishAsync(string stream, byte[] value, AckHandler ackHandler, MessageOptions opts, CancellationToken cancellationToken = default)
        {
            if (opts.CorrelationId == string.Empty)
            {
                opts = opts with { CorrelationId = Guid.NewGuid().ToString() };
            }

            var partition = opts.Partition;
            if (opts.Partitioner is not null)
            {
                if (!Metadata.HasStreamInfo(stream))
                {
                    throw new StreamNotExistsException("No metadata for stream");
                }
                partition = opts.Partitioner.Partition(stream, opts.Key, value, Metadata);
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
                var ackCtx = new AckContext
                {
                    Handler = ackHandler,
                };
                cancellationToken.Register(async () =>
                {
                    var ctx = RemoveAckContext(request.CorrelationId);
                    if (ctx is not null)
                    {
                        await ctx.Handler(null);
                    }
                });
                lock (AckContexts)
                {
                    AckContexts = AckContexts.Add(request.CorrelationId, ackCtx);
                }
            }

            var broker = Brokers.GetFromStream(stream, partition);
            try
            {
                await broker.Stream.RequestStream.WriteAsync(request);
            }
            catch (Grpc.Core.RpcException ex)
            {
                RemoveAckContext(request.CorrelationId);
                if (ex.StatusCode == Grpc.Core.StatusCode.FailedPrecondition)
                {
                    throw new ReadOnlyException("The partition is readonly", ex);
                }
            }
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
                if (context is not null)
                {
                    return context.Handler(message);
                }
            }
            return null;
        }

        private AckContext RemoveAckContext(string correlationID)
        {
            lock (AckContexts)
            {
                if (!AckContexts.ContainsKey(correlationID))
                {
                    return null;
                }
                var ctx = AckContexts[correlationID];
                AckContexts = AckContexts.Remove(correlationID);
                return ctx;
            }
        }

        /// <summary>
        /// Subscribe is used to consume streams.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="opts"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>Returns <see cref="Subscription" /></returns>
        public async Task<Subscription> Subscribe(string stream, SubscriptionOptions opts, CancellationToken cancellationToken = default)
        {
            for (var i = 1; i <= SubscriptionResiliencyTryCount; i++)
            {
                await UpdateMetadataCache(cancellationToken);
                try
                {
                    var address = Metadata.GetAddress(stream, opts.Partition, opts.ReadIsrReplica);
                    var broker = Brokers.GetFromAddress(address);

                    var request = new Proto.SubscribeRequest
                    {
                        Stream = stream,
                        StartPosition = opts.StartPosition,
                        StartOffset = opts.StartOffset,
                        StartTimestamp = opts.StartTimestamp.ToUnixTimeMilliseconds() * 1_000_000,
                        StopPosition = opts.StopPosition,
                        StopOffset = opts.StopOffset,
                        StopTimestamp = opts.StopTimestamp.ToUnixTimeMilliseconds() * 1_000_000,
                        Partition = opts.Partition,
                        ReadISRReplica = opts.ReadIsrReplica,
                    };
                    var rpcSub = broker.Client.Subscribe(request);
                    // Server responds with an empty message or error.
                    // Retry on error.
                    try
                    {
                        await rpcSub.ResponseStream.MoveNext(cancellationToken);
                    }
                    catch (Grpc.Core.RpcException)
                    {
                        continue;
                    }

                    var subscription = new Subscription(rpcSub, cancellationToken);
                    return subscription;
                }
                catch (BrokerNotFoundException)
                {
                    await Task.Delay(50);
                    await UpdateMetadataCache();
                }
            }
            throw new BrokerNotFoundException();
        }
    }
}
