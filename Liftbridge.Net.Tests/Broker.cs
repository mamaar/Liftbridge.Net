using System;
using Xunit;
using Liftbridge.Net;

namespace Liftbridge.Net.Tests
{
    public class BrokerAddressTests
    {
        [Fact]
        public void TestBrokerAddressToString()
        {
            var address = new BrokerAddress { Host = "localhost", Port = 9292 };
            Assert.Equal("localhost:9292", address.ToString());
        }

        [Fact]
        public void TestBrokerAddressEquals()
        {
            Assert.Equal(new BrokerAddress { Host = "localhost", Port = 9292, }, new BrokerAddress { Host = "localhost", Port = 9292, });
        }
    }
}
