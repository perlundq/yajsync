/*
 * Integer packing encoding/decoding routines
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013, 2014 Per Lundqvist
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.perlundq.yajsync.session;

import java.nio.ByteBuffer;

import com.github.perlundq.yajsync.channels.Readable;
import com.github.perlundq.yajsync.util.BitOps;

final class IntegerCoder
{
    private static final byte[] _int_byte_extra = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* (00 - 3F)/4 */
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* (40 - 7F)/4 */
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* (80 - BF)/4 */
        2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 5, 6, /* (C0 - FF)/4 */
    };

    private IntegerCoder() {}

    /**
     * 0   = [0]
     * ...
     * 127 = [127]
     * 128 = [128,128]
     * 129 = [128,129]
     * ...
     * 255 = [128,255]
     * 256 = [129,0]
     * 16383 (2**14 - 1) = [191,255]
     * 16384 (2**14)     = [192,0,64]
     * 65536 (2**16)     = [193,0,0]
     *
     */
    public static ByteBuffer encodeLong(long value, int minBytes)
    {
        assert minBytes >= 1 && minBytes <= 8;

        byte[] buf = new byte[9];
        BitOps.putLongAsLittleEndian(buf, 1, value);

        int count = buf.length - 1; // int lastNonZeroByteIndex = count;
        while (count > minBytes && buf[count] == 0) {
            count--;
        }

        int firstByteValue = 0xFF & (1 << (7 - count + minBytes));
        // 1 <= minBytes <= 8
        // minBytes <= count <= 8
        // max(-count + minBytes) when count equals minBytes
        // min(-count + minBytes) == -8 + 1 = -7
        // 1 << ( 7 - 8 + 1) == 1 << 0 == 1;
        // 1 << ( 7 - 0 ) == 128
        // firstByteValue is a power of 2 in range: 1 <= firstByteValue <= 128

        if ((0xFF & buf[count]) >= firstByteValue) {
            buf[0] = (byte) ~ (firstByteValue - 1);
            count++;
        } else if (count > minBytes) {
            buf[0] = (byte) ((~ (firstByteValue * 2 - 1)) | buf[count]);
        } else {
            buf[0] = buf[count];
        }

        return ByteBuffer.wrap(buf, 0, count);
    }

    public static long decodeLong(Readable src, int minBytes) throws Exception
    {
        assert minBytes >= 1 && minBytes <= 8;

        byte[] buf = new byte[10];
        src.get(buf, 0, minBytes);
        int ch = 0xFF & buf[0];
        int extra = _int_byte_extra[ch / 4];

        if (extra > 0) {
            src.get(buf, minBytes, extra);
            int bit = 1 << (8 - extra); // 2**3, 2**4, ..., 2**7 (power of 2 between 8-128)
            buf[minBytes + extra] = (byte) (ch & (bit - 1));
        } else {
            buf[minBytes + extra] = (byte) ch;
        }
        return BitOps.toBigEndianLong(buf, 1);
    }
}
