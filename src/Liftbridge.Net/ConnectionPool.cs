using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Liftbridge.Net
{
    public class Broker
    {
        public Channel Channel { get; init; }
        public Proto.API.APIClient Client { get; init; }
        public AsyncDuplexStreamingCall<Proto.PublishRequest, Proto.PublishResponse> Stream { get; init; }

        public Broker(BrokerAddress address, AckHandler ackHandler)
        {
            Channel = new Channel(address.Host, address.Port, Grpc.Core.ChannelCredentials.Insecure);
            Client = new Proto.API.APIClient(Channel);
            Stream = Client.PublishAsync();

            Task.Run(async () =>
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

    public class Brokers
    {
        ImmutableDictionary<BrokerAddress, Broker> addressConnectionPool { get; set; }
        AckHandler AckReceivedHandler { get; set; }


        public Brokers(IEnumerable<BrokerAddress> addresses, AckHandler ackHandler)
        {
            addressConnectionPool = ImmutableDictionary<BrokerAddress, Broker>.Empty;
            AckReceivedHandler = ackHandler;
            Update(addresses);
        }


        public Task Update(IEnumerable<BrokerAddress> addresses)
        {
            lock (addressConnectionPool)
            {
                ImmutableDictionary<BrokerAddress, Broker> newBrokers = ImmutableDictionary<BrokerAddress, Broker>.Empty;
                foreach (var address in addresses)
                {
                    if (!addressConnectionPool.ContainsKey(address))
                    {
                        newBrokers = newBrokers.Add(address, new Broker(address, AckReceivedHandler));
                    }
                    else
                    {
                        newBrokers = newBrokers.Add(address, addressConnectionPool[address]);
                    }
                }

                var closeOldConnections = Task.WhenAll(addressConnectionPool
                    .Where(broker => !newBrokers.ContainsKey(broker.Key))
                    .Select(broker => broker.Value.Channel.ShutdownAsync()));

                addressConnectionPool = newBrokers;
                return closeOldConnections;
            }
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
    }
}
