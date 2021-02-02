using System;
using System.Linq;

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
