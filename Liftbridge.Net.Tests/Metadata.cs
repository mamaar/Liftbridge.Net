using System;
using Xunit;
using Liftbridge.Net;

namespace Liftbridge.Net.Tests
{
    public class MetadataTests
    {
        [Fact]
        public void TestGetActualBroker()
        {
            var metadata = new Metadata { };
            var b = new BrokerInfo { Id = "1", Host = "localhost", Port = 9292, };
            metadata = metadata with { Brokers = metadata.Brokers.Add(b)};

            var broker = metadata.GetBroker("1");
            Assert.Equal(b, broker);
        }
    }
}
