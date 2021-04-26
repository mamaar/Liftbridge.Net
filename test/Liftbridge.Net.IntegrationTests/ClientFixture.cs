namespace Liftbridge.Net.IntegrationTests
{
    public class ClientFixture
    {
        public Liftbridge.Net.ClientAsync Client { get; set; }
        public System.Threading.CancellationToken TimeoutToken { get; set; }

        public ClientFixture()
        {
            var options = new ClientOptions
            {
                Brokers = new[] {
                    new BrokerAddress { Host = "localhost", Port = 9292 },
                }
            };
            Client = new ClientAsync(options);

            var tokenSource = new System.Threading.CancellationTokenSource();
            tokenSource.CancelAfter(System.TimeSpan.FromMilliseconds(5000));
            TimeoutToken = tokenSource.Token;
        }
    }

    [Xunit.CollectionDefinition("Client collection")]
    public class ClientCollection : Xunit.ICollectionFixture<ClientFixture>
    {

    }
}
