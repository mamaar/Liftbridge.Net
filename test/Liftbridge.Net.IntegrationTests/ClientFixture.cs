namespace Liftbridge.Net.IntegrationTests
{
    public class ClientFixture
    {
        public Liftbridge.Net.ClientAsync Client { get; set; }

        public ClientFixture()
        {
            var options = new ClientOptions
            {
                Brokers = new[] {
                    new BrokerAddress { Host = "localhost", Port = 9292 },
                }
            };
            Client = new ClientAsync(options);
        }
    }

    [Xunit.CollectionDefinition("Client collection")]
    public class ClientCollection : Xunit.ICollectionFixture<ClientFixture>
    {

    }
}
