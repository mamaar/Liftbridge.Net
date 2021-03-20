using Grpc.Core;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;


namespace Liftbridge.Net
{
    public record BrokerAddress
    {
        public string Host { get; init; }
        public int Port { get; init; }

        override public string ToString()
        {
            return $"{Host}:{Port}";
        }
    }

    public record BrokerInfo
    {
        public string Id { get; init; }
        public string Host { get; init; }
        public int Port { get; init; }

        public BrokerAddress GetAddress()
        {
            return new BrokerAddress { Host = Host, Port = Port };
        }

        public static BrokerInfo FromProto(Proto.Broker broker)
        {
            return new BrokerInfo
            {
                Id = broker.Id,
                Host = broker.Host,
                Port = broker.Port,
            };
        }
    }

    public class Broker
    {
        public Channel Channel { get; init; }
        public Proto.API.APIClient Client { get; init; }
        public AsyncDuplexStreamingCall<Proto.PublishRequest, Proto.PublishResponse> Stream { get; set; }
        private System.Threading.SemaphoreSlim publishSemaphore = new System.Threading.SemaphoreSlim(1);
        private ImmutableDictionary<string, AckContext> AckContexts;

        public Broker(BrokerAddress address)
        {
            Channel = new Channel(address.Host, address.Port, ChannelCredentials.Insecure);
            Client = new Proto.API.APIClient(Channel);
            Stream = Client.PublishAsync();
            AckContexts = ImmutableDictionary<string, AckContext>.Empty;
            _ = Task.Run(AckTask);
        }

        private async Task AckTask()
        {
            while (await Stream.ResponseStream.MoveNext())
            {
                var message = Stream.ResponseStream.Current;
                var ctx = RemoveAckContext(message.CorrelationId ?? message.Ack.CorrelationId);
                ctx?.Handler.Invoke(message);
            }
            return;
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
        /// Safe call for writing to the stream if the caller doesn't await.
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public async Task Publish(Proto.PublishRequest request, Func<Proto.PublishResponse, Task> ackHandler, System.Threading.CancellationToken cancellationToken = default)
        {
            await publishSemaphore.WaitAsync();


            if (ackHandler is not null)
            {
                lock (AckContexts)
                {
                    var ackCtx = new AckContext
                    {
                        Handler = ackHandler,
                    };
                    AckContexts = AckContexts.Add(request.CorrelationId, ackCtx);
                }
                cancellationToken.Register(() =>
                {
                    _ = RemoveAckContext(request.CorrelationId);
                });
            }

            await Stream.RequestStream.WriteAsync(request);
            publishSemaphore.Release();
        }

        public Task Close()
        {
            return Task.WhenAll(Channel.ShutdownAsync(), Stream.RequestStream.CompleteAsync());
        }
    }

    public class Brokers : IEnumerable<Broker>
    {
        ImmutableDictionary<BrokerAddress, Broker> addressConnectionPool { get; set; }
        Func<Proto.PublishResponse, Task> AckReceivedHandler { get; set; }
        private System.Threading.SemaphoreSlim updateSemaphore = new System.Threading.SemaphoreSlim(1);


        public Brokers(IEnumerable<BrokerAddress> addresses)
        {
            addressConnectionPool = ImmutableDictionary<BrokerAddress, Broker>
                .Empty
                .AddRange(addresses.Select(address =>
                    new KeyValuePair<BrokerAddress, Broker>(address, new Broker(address))
                ));
        }


        public async Task Update(IEnumerable<BrokerAddress> addresses, System.Threading.CancellationToken cancellationToken = default)
        {
            await updateSemaphore.WaitAsync();
            ImmutableDictionary<BrokerAddress, Broker> newBrokers = ImmutableDictionary<BrokerAddress, Broker>.Empty;
            foreach (var address in addresses)
            {
                if (!addressConnectionPool.ContainsKey(address))
                {
                    var broker = new Broker(address);
                    newBrokers = newBrokers.Add(address, broker);
                }
                else
                {
                    newBrokers = newBrokers.Add(address, addressConnectionPool[address]);
                }
            }
            var closeOldConnections = addressConnectionPool
                .Where(broker => !newBrokers.ContainsKey(broker.Key))
                .Select(broker => broker.Value.Channel.ShutdownAsync());
            lock (addressConnectionPool)
            {
                addressConnectionPool = newBrokers;
            }
            updateSemaphore.Release();
            await Task.WhenAll(closeOldConnections);
        }

        public Broker GetFromAddress(BrokerAddress address)
        {
            return addressConnectionPool[address];
        }

        public Broker GetRandom()
        {
            var rand = new Random();
            var n = rand.Next(0, addressConnectionPool.Count);
            var key = addressConnectionPool.Keys.ElementAt(n);
            return addressConnectionPool[key];
        }

        public Broker GetFromStream(string stream, int partition)
        {
            var hash = System.Text.Encoding.ASCII.GetBytes($"{stream}:{partition}");
            var n = (int)(Dexiom.QuickCrc32.QuickCrc32.Compute(hash) % addressConnectionPool.Count);

            var key = addressConnectionPool.Keys.ElementAt(n);
            return addressConnectionPool[key];
        }

        public Task CloseAll()
        {
            return Task.WhenAll(
                addressConnectionPool.Select(pair =>
                {
                    return pair.Value.Close();
                })
            );
        }

        public IEnumerator<Broker> GetEnumerator()
        {
            return addressConnectionPool.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
