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

        public Broker(BrokerAddress address, AckHandler ackHandler)
        {
            Channel = new Channel(address.Host, address.Port, ChannelCredentials.Insecure);
            Client = new Proto.API.APIClient(Channel);
            Stream = Client.PublishAsync();

            _ = Task.Run(async () =>
            {
                while (await Stream.ResponseStream.MoveNext())
                {
                    var message = Stream.ResponseStream.Current;
                    if (ackHandler is not null)
                    {
                        await ackHandler(message);
                    }
                }
            });
        }

        public Task Close()
        {
            return Channel.ShutdownAsync();
        }
    }

    public class Brokers : IEnumerable<Broker>
    {
        ImmutableDictionary<BrokerAddress, Broker> addressConnectionPool { get; set; }
        AckHandler AckReceivedHandler { get; set; }


        public Brokers(IEnumerable<BrokerAddress> addresses, AckHandler ackHandler)
        {
            addressConnectionPool = ImmutableDictionary<BrokerAddress, Broker>
                .Empty
                .AddRange(addresses.Select(address =>
                    new KeyValuePair<BrokerAddress, Broker>(address, new Broker(address, ackHandler))
                ));
            AckReceivedHandler = ackHandler;
        }


        public Task Update(IEnumerable<BrokerAddress> addresses, System.Threading.CancellationToken cancellationToken = default)
        {
            ImmutableDictionary<BrokerAddress, Broker> newBrokers = ImmutableDictionary<BrokerAddress, Broker>.Empty;
            foreach (var address in addresses)
            {
                if (!addressConnectionPool.ContainsKey(address))
                {
                    var broker = new Broker(address, AckReceivedHandler);
                    newBrokers = newBrokers.Add(address, broker);
                }
                else
                {
                    newBrokers = newBrokers.Add(address, addressConnectionPool[address]);
                }
            }
            var closeOldConnections = addressConnectionPool
                .Where(broker => !newBrokers.ContainsKey(broker.Key))
                .Select(broker => broker.Value.Channel.ShutdownAsync()) ;
            lock (addressConnectionPool)
            {
                addressConnectionPool = newBrokers;
            }
            return Task.WhenAll(closeOldConnections);
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
