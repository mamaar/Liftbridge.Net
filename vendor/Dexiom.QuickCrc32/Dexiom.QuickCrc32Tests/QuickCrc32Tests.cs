using Microsoft.VisualStudio.TestTools.UnitTesting;
using Dexiom.QuickCrc32;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dexiom.QuickCrc32.Tests
{
    [TestClass()]
    public class QuickCrc32Tests
    {
        [TestMethod()]
        public void ComputeToBase64Test()
        {
            //ref: http://www.sunshine2k.de/coding/javascript/crc/crc_js.html
            //ref: http://www.zorc.breitbandkatze.de/crc.html

            var bytes = Encoding.ASCII.GetBytes("The quick brown fox jumps over the lazy dog");
            Assert.IsTrue(QuickCrc32.ComputeToString(bytes) == "414FA339");
        }
        [TestMethod()]
        public void ComputeSomeTests()
        {
            //ref: http://www.sunshine2k.de/coding/javascript/crc/crc_js.html
            //ref: http://www.zorc.breitbandkatze.de/crc.html

            byte[] bytes;
            bytes = Encoding.ASCII.GetBytes("Hello world!");
            Assert.IsTrue(QuickCrc32.Compute(bytes) == 461707669);


            bytes = Encoding.ASCII.GetBytes("Hello world.");
            Assert.IsTrue(QuickCrc32.Compute(bytes) == 2335835140);
        }
    }
}