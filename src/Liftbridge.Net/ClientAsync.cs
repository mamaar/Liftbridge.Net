﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;



namespace Liftbridge.Net
{
    using AckPolicy = Proto.AckPolicy;

    public class ClientOptions
    {
        public IEnumerable<BrokerAddress> Brokers { get; init; }
        public string TLSCert { get; init; }
        public TimeSpan ResubscribeWaitTime { get; init; }
        public TimeSpan AckWaitTime { get; init; }
    }

    public class ClientAsync
    {
        private const int RPCResiliencyTryCount = 10;
        private const int SubscriptionResiliencyTryCount = 5;
        private const string CursorsStream = "__cursors";

        private Brokers Brokers { get; set; }
        private MetadataCache Metadata { get; set; }

        private readonly ClientOptions Options;

        public ClientAsync(ClientOptions opts)
        {
            Options = opts;
            Metadata = new MetadataCache();
            Brokers = new Brokers(opts.Brokers);
        }

        private async Task<T> DoResilientRPC<T>(Func<Proto.API.APIClient, CancellationToken, Task<T>> func, CancellationToken cancellationToken = default)
        {
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
                    if (ex.StatusCode != Grpc.Core.StatusCode.Unavailable)
                    {
                        throw;
                    }
                    try
                    {
                        var cancelTokenSource = new CancellationTokenSource();
                        cancelTokenSource.CancelAfter(1000);
                        await UpdateMetadataCache(cancelTokenSource.Token);
                    }
                    catch (Grpc.Core.RpcException)
                    {
                        continue;
                    }
                }
            }
            throw new ConnectionErrorException();
        }

        private async Task<T> DoResilientLeaderRPC<T>(string stream, int partition, Func<Proto.API.APIClient, CancellationToken, Task<T>> func, CancellationToken cancellationToken = default)
        {
            if (!Metadata.HasBrokers() || !Metadata.HasStreamInfo(stream))
            {
                await UpdateMetadataCache(new[] { stream }, cancellationToken);
            }
            for (var i = 0; i < RPCResiliencyTryCount; i++)
            {
                var leaderBrokerInfo = Metadata.GetLeader(stream, partition);
                var leaderBroker = Brokers.GetFromAddress(leaderBrokerInfo.Address);
                try
                {
                    return await func(leaderBroker.Client, cancellationToken);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.Unavailable)
                    {
                        try
                        {
                            var cancelTokenSource = new CancellationTokenSource();
                            cancelTokenSource.CancelAfter(1000);
                            await UpdateMetadataCache(new[] { stream }, cancelTokenSource.Token);
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

        public Task<Metadata> FetchMetadata(CancellationToken cancellationToken = default)
        {
            return FetchMetadata(Array.Empty<string>(), cancellationToken);
        }

        public async Task<Metadata> FetchMetadata(IEnumerable<string> streams, CancellationToken cancellationToken = default)
        {
            var request = new Proto.FetchMetadataRequest { };
            request.Streams.AddRange(streams);
            foreach (var broker in Brokers)
            {
                var meta = await broker.Client.FetchMetadataAsync(request, cancellationToken: cancellationToken);
                var brokers = meta.Brokers
                        .Select(broker => KeyValuePair.Create(broker.Id, BrokerInfo.FromProto(broker)))
                        .ToImmutableDictionary();
                var streamMetadata = meta.Metadata
                    .Where(s => s.Error == Proto.StreamMetadata.Types.Error.Ok)
                    .Select(stream => new KeyValuePair<string, StreamInfo>(stream.Name, StreamInfo.FromProto(stream)))
                    .ToImmutableDictionary();
                return new Metadata
                {
                    LastUpdated = DateTime.UtcNow,
                    Brokers = brokers,
                    Streams = streamMetadata,
                };
            }

            return new Metadata() { LastUpdated = DateTime.MinValue };
        }

        private async Task UpdateMetadataCache(CancellationToken cancellationToken = default)
        {
            await UpdateMetadataCache(Array.Empty<string>(), cancellationToken);
        }

        private async Task UpdateMetadataCache(IEnumerable<string> streams, CancellationToken cancellationToken = default)
        {
            await Metadata.Update(streams, async () =>
            {
                return await FetchMetadata(streams, cancellationToken);
            });
            await Brokers.Update(Metadata.GetAddresses(), cancellationToken);
        }

        public async Task<PartitionInfo> FetchPartitionMetadata(string stream, int partition, CancellationToken cancellationToken = default)
        {
            return await DoResilientLeaderRPC(stream, partition, async (client, cancelToken) =>
            {
                var request = new Proto.FetchPartitionMetadataRequest { Stream = stream, Partition = partition };
                var result = await client.FetchPartitionMetadataAsync(request, cancellationToken: cancelToken);

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
            }, cancellationToken);
        }

        /// <summary>
        /// Returns whether a stream exists in the cluster.
        /// Will send a request to the server if the stream is not found in the cache.
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public async Task<bool> StreamExists(string stream)
        {
            if (!Metadata.HasStreamInfo(stream))
            {
                await UpdateMetadataCache(new[] { stream });
            }
            return Metadata.HasStreamInfo(stream);
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
                    var result = await client.DeleteStreamAsync(request, cancellationToken: cancelToken);
                    // Removes the stream from cache
                    Metadata.RemoveStream(stream);
                    return result;
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
                _ = PublishAsync(stream, value, opts, ackHandler: null, cancellationToken: cancellationToken);
                return null;
            }

            var ackOnce = new WriteOnceBlock<Proto.Ack>(null);

            // Publish and wait for ack or timeout
            await PublishAsync(stream, value, opts, ackHandler: (message) =>
            {
                ackOnce.Post(message.Ack);
            }, cancellationToken: cancellationToken);
            return await ackOnce.ReceiveAsync(cancellationToken);
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
        public Task PublishAsync(string stream, byte[] value, MessageOptions opts, Action<Proto.PublishResponse> ackHandler, CancellationToken cancellationToken = default)
        {
            var message = Message.Default with
            {
                Stream = stream,
                Value = value,
            };

            return PublishAsync(message, opts, ackHandler, cancellationToken);
        }

        public async Task PublishAsync(Message message, MessageOptions opts, Action<Proto.PublishResponse> ackHandler, CancellationToken cancellationToken = default)
        {
            await UpdateMetadataCache(new[] { message.Stream }, cancellationToken);
            if (opts is null)
            {
                opts = MessageOptions.Default;
            }
            if (opts.CorrelationId == string.Empty)
            {
                opts = opts with { CorrelationId = Guid.NewGuid().ToString() };
            }

            var partition = opts.Partition;
            if (opts.Partitioner is not null)
            {
                if (!Metadata.HasStreamInfo(message.Stream))
                {
                    throw new StreamNotExistsException("No metadata for stream");
                }
                partition = opts.Partitioner.Partition(message.Stream, opts.Key, message.Value, Metadata);
            }

            var key = opts.Key == null ? Google.Protobuf.ByteString.Empty : Google.Protobuf.ByteString.CopyFrom(opts.Key);
            var valueBytes = message.Value == null ? Google.Protobuf.ByteString.Empty : Google.Protobuf.ByteString.CopyFrom(message.Value);

            var request = new Proto.PublishRequest
            {
                Stream = message.Stream,
                Partition = partition,
                Key = key,
                Value = valueBytes,
                AckInbox = opts.AckInbox,
                CorrelationId = opts.CorrelationId,
                AckPolicy = opts.AckPolicy,
                ExpectedOffset = opts.ExpectedOffset,
            };

            var broker = Brokers.GetFromStream(message.Stream, partition);
            try
            {
                await broker.Publish(request, ackHandler, cancellationToken);
            }
            catch (Grpc.Core.RpcException ex)
            {
                if (ex.StatusCode == Grpc.Core.StatusCode.FailedPrecondition)
                {
                    throw new ReadOnlyException("The partition is readonly", ex);
                }
            }
        }

        public async Task PublishToSubject(string subject, byte[] value, MessageOptions opts, CancellationToken cancellationToken = default)
        {
            var key = opts.Key == null ? Google.Protobuf.ByteString.Empty : Google.Protobuf.ByteString.CopyFrom(opts.Key);
            var request = new Proto.PublishToSubjectRequest
            {
                Subject = subject,
                Value = Google.Protobuf.ByteString.CopyFrom(value),
                Key = key,
                AckInbox = opts.AckInbox,
                CorrelationId = opts.CorrelationId,
                AckPolicy = opts.AckPolicy,
            };

            await DoResilientRPC(async (client, cancellationToken) =>
            {
                var response = await client.PublishToSubjectAsync(request, cancellationToken: cancellationToken);
                return response;
            }, cancellationToken);
        }

        /// <summary>
        /// Subscribe is used to consume streams.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="opts"></param>
        /// <param name="cancellationToken"></param>
        public async IAsyncEnumerable<Message> Subscribe(string stream, SubscriptionOptions opts, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            for (var i = 1; i <= SubscriptionResiliencyTryCount; i++)
            {
                await UpdateMetadataCache(new[] { stream }, cancellationToken);
                if (!Metadata.TryGetBroker(stream, opts.Partition, opts.ReadIsrReplica, out BrokerInfo brokerInfo))
                {
                    continue;
                }
                if (!Brokers.TryGetFromAddress(brokerInfo.Address, out Broker broker))
                {
                    continue;
                }

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
                var client = broker.CreateClient();
                var rpcSub = client.Subscribe(request, cancellationToken: cancellationToken);
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

                while (true)
                {

                    try
                    {
                        await rpcSub.ResponseStream.MoveNext(cancellationToken);
                    }
                    catch (Grpc.Core.RpcException ex)
                    {
                        switch (ex.StatusCode)
                        {
                            case Grpc.Core.StatusCode.Cancelled:
                                break;
                            case Grpc.Core.StatusCode.NotFound:
                                throw new StreamNotExistsException();
                            case Grpc.Core.StatusCode.FailedPrecondition:
                                throw new StreamPausedException();
                            case Grpc.Core.StatusCode.ResourceExhausted:
                                throw new EndOfReadonlyException();
                            case Grpc.Core.StatusCode.Unavailable:
                                throw new BrokerNotFoundException();
                        }
                        throw;
                    }

                    yield return Message.FromProto(rpcSub.ResponseStream.Current);
                }
            }
        }

        static byte[] GetCursorKey(string cursorId, string stream, int partition)
        {
            return System.Text.Encoding.ASCII.GetBytes($"{cursorId},{stream},{partition}");
        }

        private static int GetCursorPartition(byte[] cursorKey, StreamInfo cursorStreamInfo)
        {
            return (int)Dexiom.QuickCrc32.QuickCrc32.Compute(cursorKey) % cursorStreamInfo.Partitions.Count;
        }

        public async Task SetCursor(string cursorId, string stream, int partition, long offset, CancellationToken cancellationToken = default)
        {
            if(!await StreamExists(CursorsStream))
            {
                throw new CursorsDisabledException("cursors are not enabled in the Liftbridge cluster");
            }
            if(!await StreamExists(stream))
            {
                throw new StreamNotExistsException();
            }
            var cursorKey = GetCursorKey(cursorId, stream, partition);
            var cursorStreamInfo = Metadata.GetStreamInfo(CursorsStream);
            var cursorPartition = GetCursorPartition(cursorKey, cursorStreamInfo);

            var request = new Proto.SetCursorRequest
            {
                CursorId = cursorId,
                Stream = stream,
                Partition = partition,
                Offset = offset,
            };
            await DoResilientLeaderRPC(CursorsStream, cursorPartition, async (client, cancelToken) =>
            {
                return await client.SetCursorAsync(request, cancellationToken: cancelToken);
            }, cancellationToken);
        }

        public async Task<long> FetchCursor(string cursorId, string stream, int partition, CancellationToken cancellationToken = default)
        {
            if (!await StreamExists(CursorsStream))
            {
                throw new CursorsDisabledException("cursors are not enabled in the Liftbridge cluster");
            }
            var cursorKey = GetCursorKey(cursorId, stream, partition);
            var cursorStreamInfo = Metadata.GetStreamInfo(CursorsStream);
            var cursorPartition = GetCursorPartition(cursorKey, cursorStreamInfo);

            var request = new Proto.FetchCursorRequest
            {
                CursorId = cursorId,
                Stream = stream,
                Partition = partition,
            };
            return await DoResilientLeaderRPC(CursorsStream, cursorPartition, async (client, cancelToken) =>
            {
                var response = await client.FetchCursorAsync(request, cancellationToken: cancelToken);
                return response.Offset;
            }, cancellationToken);
        }
    }
}
