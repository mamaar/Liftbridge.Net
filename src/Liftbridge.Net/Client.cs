using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Liftbridge.Net
{
    public class LiftbridgeException : Exception
    {
        public LiftbridgeException() { }
        public LiftbridgeException(string message) : base(message) { }
        public LiftbridgeException(string message, Exception inner) : base(message, inner) { }
    }
    public class ConnectionErrorException : LiftbridgeException { }
    public class StreamNotExistsException : LiftbridgeException { }
    public class StreamAlreadyExistsException : LiftbridgeException { }
    public class BrokerNotFoundException : LiftbridgeException { }

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

        private Proto.API.APIClient internalClient;
        private Grpc.Core.Channel internalChannel;

        private Metadata metadata;

        private ImmutableDictionary<BrokerAddress, ConnectionPool> clientPools;

        private ClientOptions options;

        public Client(ClientOptions opts)
        {
            options = opts;
            metadata = new Metadata
            {
                Brokers = ImmutableHashSet<BrokerInfo>.Empty,
                BootstrapAddresses = options.Brokers.ToImmutableHashSet(),
            };

            CreateRpcClient();
        }

        static Grpc.Core.Channel CreateChannel(IEnumerable<BrokerAddress> addresses)
        {
            var rand = new Random();
            var addressIndex = rand.Next(0, addresses.Count());
            var address = addresses.ElementAt(addressIndex);
            return new Grpc.Core.Channel(address.Host, address.Port, Grpc.Core.ChannelCredentials.Insecure);
        }

        Proto.API.APIClient CreateRpcClient()
        {
            internalChannel = CreateChannel(metadata.GetAddresses());
            internalClient = new Proto.API.APIClient(internalChannel);
            return internalClient;
        }

        private ConnectionPool GetPoolForAddress(BrokerAddress address)
        {
            if (!clientPools.ContainsKey(address))
            {
                clientPools = clientPools.Add(address, new ConnectionPool(options.MaxConnsPerBroker, options.KeepAliveTime));
            }
            return clientPools[address];
        }

        private ConnectionPool GetPoolForStreamPartition(string stream, int partition, bool isISRReplica)
        {
            var address = metadata.GetAddress(stream, partition, isISRReplica);
            var pool = GetPoolForAddress(address);
            return pool;
        }

        private T DoResilientRPC<T>(Func<Proto.API.APIClient, T> func)
        {
            for (var i = 0; i < RPCResiliencyTryCount; i++)
            {
                try
                {
                    return func(internalClient);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.Unavailable)
                    {
                        // Reconnect before next try.
                        CreateRpcClient();
                        continue;
                    }
                    throw;
                }
            }
            throw new ConnectionErrorException();
        }

        private async Task<T> DoResilientRPCAsync<T>(Func<Proto.API.APIClient, Task<T>> func)
        {
            for (var i = 0; i < RPCResiliencyTryCount; i++)
            {
                try
                {
                    return await func(internalClient);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.Unavailable)
                    {
                        // Reconnect before next try.
                        CreateRpcClient();
                        continue;
                    }
                    throw;
                }
            }
            throw new ConnectionErrorException();
        }

        private async Task DoResilientLeaderRPC(string stream, int partition, Func<Proto.API.APIClient, Task> func)
        {
            await func(internalClient);
        }

        public async Task<Metadata> FetchMetadataAsync()
        {
            return await DoResilientRPC(async client =>
            {
                var meta = await client.FetchMetadataAsync(new Proto.FetchMetadataRequest { }, deadline: DateTime.UtcNow.AddSeconds(3));
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

        public async Task FetchPartitionMetadataAsync(string stream, int partition)
        {
            PartitionInfo partitionInfo;
            await DoResilientLeaderRPC(stream, partition, async client =>
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

                partitionInfo = new PartitionInfo
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

        public void CreateStream(string name, string subject)
        {
            var opts = new StreamOptions();
            CreateStream(name, subject, opts);
        }

        public void CreateStream(string name, string subject, StreamOptions streamOptions)
        {
            var request = streamOptions.Request();
            request.Name = name;
            request.Subject = subject;

            DoResilientRPC(client =>
            {
                try
                {
                    return client.CreateStream(request);
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

        public void DeleteStream(string name)
        {
            var request = new Proto.DeleteStreamRequest { Name = name };
            DoResilientRPC(client =>
            {
                try
                {
                    return client.DeleteStream(request);
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

        public void PauseStream(string name, IEnumerable<int> partitions = null, bool resumeAll = false)
        {
            var request = new Proto.PauseStreamRequest { Name = name, ResumeAll = resumeAll };
            if (partitions != null)
            {
                request.Partitions.AddRange(partitions);
            }
            DoResilientRPC(client =>
            {
                try
                {
                    return client.PauseStream(request);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if (ex.StatusCode == Grpc.Core.StatusCode.NotFound)
                    {
                        throw new StreamNotExistsException();
                    }
                    throw;
                }
            }
            );
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
            }
);
        }

        public async Task Subscribe(string stream, SubscriptionOptions opts, MessageHandler messageHandler)
        {
            for (var i = 1; i <= SubscriptionResiliencyTryCount; i++)
            {
                try
                {
                    var address = metadata.GetAddress(stream, opts.Partition, opts.ReadIsrReplica);
                    if (!clientPools.ContainsKey(address))
                    {
                        clientPools = clientPools.Add(address, new ConnectionPool(options.MaxConnsPerBroker, options.KeepAliveTime));
                    }
                    var connectionPool = clientPools[address];

                    var channel = connectionPool.Get(() =>
                    {
                        return CreateChannel(new List<BrokerAddress> { address });
                    });
                    var subscriptionClient = new Proto.API.APIClient(channel);
                    var request = new Proto.SubscribeRequest
                    {
                        Partition = opts.Partition,
                        ReadISRReplica = opts.ReadIsrReplica,
                    };
                    using var subscription = subscriptionClient.Subscribe(request);
                    while (await subscription.ResponseStream.MoveNext(default))
                    {
                        var message = subscription.ResponseStream.Current;
                        await messageHandler(message);
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
                    metadata = await FetchMetadataAsync();
                }
            }
        }
    }

    public record SubscriptionOptions
    {
        public int Partition { get; init; }
        public bool ReadIsrReplica { get; init; }
    }
}
