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
}
