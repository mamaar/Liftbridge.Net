using System;
using System.Linq;

/* MIT License

Copyright (c) 2017 Dexiom

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE. */

namespace Dexiom.QuickCrc32
{
    /// <summary>
    /// Quick and dirty CRC32 calculation
    /// </summary>
    /// <remarks>
    /// Based on http://sanity-free.org/12/crc32_implementation_in_csharp.html
    /// </remarks>
    public static class QuickCrc32
    {
        private static readonly uint[] Table;

        static QuickCrc32()
        {
            const uint poly = 0xedb88320;
            Table = new uint[256];
            for (uint i = 0; i < Table.Length; ++i)
            {
                var temp = i;
                for (var j = 8; j > 0; --j)
                {
                    if ((temp & 1) == 1)
                        temp = (temp >> 1) ^ poly;
                    else
                        temp >>= 1;
                }
                Table[i] = temp;
            }
        }

        public static uint Compute(byte[] bytes)
        {
            var crc = 0xffffffff;
            foreach (var t in bytes)
            {
                var index = (byte)((crc & 0xff) ^ t);
                crc = (crc >> 8) ^ Table[index];
            }
            return ~crc;
        }

        public static byte[] ComputeToBytes(byte[] bytes) => BitConverter.GetBytes(Compute(bytes)).Reverse().ToArray();

        public static string ComputeToString(byte[] bytes) => BitConverter.ToString(ComputeToBytes(bytes)).Replace("-", string.Empty);
    }
}
