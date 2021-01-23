using Liftbridge.Net;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Liftbridge.Net
{
    public class LiftbridgeException : Exception { }
    public class StreamAlreadyExistsException : LiftbridgeException { }
    public class BrokerNotFoundException: LiftbridgeException { }

    public class ClientOptions
    {
        public ImmutableHashSet<BrokerAddress> Brokers { get; init; }
        public uint MaxConnsPerBroker { get; init; }
        public TimeSpan KeepAliveTime { get; init; }
        public string TLSCert { get; init; }
        public TimeSpan ResubscribeWaitTime { get; init; }
    }

    class ConnectionPool { }

    public class Client
    {
        const int RPCResiliencyTryCount = 10;

        private Proto.API.APIClient client;

        private Metadata metadata;

        private ImmutableDictionary<BrokerAddress, ConnectionPool> clientPools;

        public Client(ClientOptions options)
        {
            metadata = new Metadata { 
                Brokers = ImmutableHashSet<BrokerInfo>.Empty,
                BootstrapAddresses = options.Brokers, 
            };
        }

        async Task<Proto.API.APIClient> createConnection() {
            var addresses = metadata.GetAddresses();

            foreach(var address in addresses)
            {
                try
                {
                    Console.WriteLine(address);
                    var channel = new Grpc.Core.Channel(address.Host, address.Port, Grpc.Core.ChannelCredentials.Insecure);
                    await channel.ConnectAsync(DateTime.UtcNow.AddSeconds(3));
                    return new Proto.API.APIClient(channel);
                }
                catch(TaskCanceledException)
                {

                }
            }
            throw new LiftbridgeException();
        }

        private ConnectionPool getPoolForAddress(BrokerAddress address)
        {
            if(!clientPools.ContainsKey(address))
            {
                clientPools = clientPools.Add(address, new ConnectionPool());
            }
            return clientPools[address];
        }

        private ConnectionPool getPoolForStreamPartition(string stream, int partition, bool isISRReplica)
        {
            var address = metadata.GetAddress(stream, partition, isISRReplica);
            var pool = getPoolForAddress(address);
            return pool;
        }

        private async Task<T> DoResilientRPC<T>(Func<Proto.API.APIClient, Task<T>> func)
        {
            if(client == null)
            {
                client = await createConnection();
            }
            for(var i = 0; i < RPCResiliencyTryCount; i++)
            {
                try
                {
                    return await func(client);
                }
                catch (Grpc.Core.RpcException ex)
                {
                    if(ex.StatusCode == Grpc.Core.StatusCode.Unavailable)
                    {
                        client = await createConnection();
                    }
                }
            }
            throw new LiftbridgeException();
        }

        private async Task DoResilientLeaderRPC(string stream, int partition, Func<Proto.API.APIClient, Task> func)
        {
            await func(client);
        }

        public async Task<Metadata> FetchMetadataAsync()
        {
            return await DoResilientRPC(async client =>
            {
                var meta = await client.FetchMetadataAsync(new Proto.FetchMetadataRequest { });
                return metadata with {
                    LastUpdated = DateTime.UtcNow,
                    Brokers = meta.Brokers
                        .Select(broker => BrokerInfo.FromProto(broker))
                        .ToImmutableHashSet(),
                    Streams = meta.Metadata.Select(stream => new KeyValuePair<string, StreamInfo>(stream.Name, StreamInfo.FromProto(stream))).ToImmutableDictionary(),
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

                var replicasInfo = ImmutableList<BrokerInfo>.Empty;
                var isrInfo = ImmutableList<BrokerInfo>.Empty;

                foreach (var replica in replicas)
                {
                    var broker = metadata.GetBroker(replica);
                    replicasInfo.Add(broker);
                }

                foreach(var isr in isrs)
                {
                    var broker = metadata.GetBroker(isr);
                    isrInfo.Add(broker);
                }

                var leaderInfo = metadata.GetBroker(leader);

                partitionInfo = new PartitionInfo {
                    Id = partitionMeta.Id,
                    Leader = leaderInfo.Id,
                    Replicas = replicasInfo.Select(replica => replica.Id).ToImmutableList(),
                    ISR = isrInfo.Select(isr => isr.Id).ToImmutableList(),
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
            CreateStreamAsync(name, subject).RunSynchronously();
        }

        public async Task CreateStreamAsync(string name, string subject)
        {
            await DoResilientRPC(async client =>
            {
                var request = new Proto.CreateStreamRequest()
                {
                    Name = name,
                    Subject = subject,
                };

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
    }
}
