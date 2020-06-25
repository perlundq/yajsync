/*
 * Rsync rolling checksum
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
package com.github.perlundq.yajsync.internal.util;

import java.nio.ByteBuffer;

public class Rolling
{
    private final static int CHAR_OFFSET = 0; // currently unused

    private Rolling() {}

    public static int compute(byte[] buf, int offset, int length)
    {
        int low16 = 0;
        int high16 = 0;

        int idx;
        for (idx = 0; idx < length - 4; idx += 4) {
            high16 += 4 * (buf[offset + idx + 0]  + low16) +
                    3 * (buf[offset + idx + 1]) +
                    2 * (buf[offset + idx + 2]) +
                    1 * (buf[offset + idx + 3]) +
                    10 * CHAR_OFFSET;
            low16 += buf[offset + idx + 0] +
                   buf[offset + idx + 1] +
                   buf[offset + idx + 2] +
                   buf[offset + idx + 3] +
                   4 * CHAR_OFFSET;
        }
        for (; idx < length; idx++) {
            low16 += buf[offset + idx] + CHAR_OFFSET;
            high16 += low16;
        }

        return toInt(low16, high16);
    }

    public static int compute(ByteBuffer buf)
    {
        int low16 = 0;
        int high16 = 0;

        int idx;
        for (idx = buf.position(); idx < buf.limit() - 4; idx += 4) {
            byte b0 = buf.get( idx + 0 );
            byte b1 = buf.get( idx + 1 );
            byte b2 = buf.get( idx + 2 );
            byte b3 = buf.get( idx + 3 );

            high16 += 4 * ( b0 + low16 ) + 3 * b1 + 2 * b2 + 1 * b3 + 10 * CHAR_OFFSET;
            
            low16 += b0 + b1 + b2 + b3 + 4 * CHAR_OFFSET;
        }
        for (; idx < buf.limit(); idx++) {
            low16 += buf.get( idx ) + CHAR_OFFSET;
            high16 += low16;
        }

        return toInt(low16, high16);
    }

    public static int add(int checksum, byte value)
    {
        int low16 = low16(checksum) + value + CHAR_OFFSET;
        int high16 = high16(checksum) + low16;
        return toInt(low16, high16);
    }

    public static int subtract(int checksum, int blockLength, byte value)
    {
        int low16 = low16(checksum) - value + CHAR_OFFSET;
        int high16 = high16(checksum) - blockLength * (value + CHAR_OFFSET);
        return toInt(low16, high16);
    }

    private static int toInt(int low16, int high16)
    {
        return (low16 & 0xFFFF) | (high16 << 16);
    }

    private static int low16(int checksum)
    {
        return 0xFFFF & checksum;
    }

    private static int high16(int checksum)
    {
        return checksum >>> 16;
    }
}