/*
 * Bitwise operation utility routines
 *
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

public final class BitOps
{
    private BitOps() {}
    
    public static int toBigEndianInt(byte[] buf)
    {
        return toBigEndianInt(buf, 0);
    }

    public static int toBigEndianInt(byte[] buf, int offset)

    {
        return ((0xFF & buf[offset + 0]) << 0)  |
               ((0xFF & buf[offset + 1]) << 8)  |
               ((0xFF & buf[offset + 2]) << 16) |
               ((0xFF & buf[offset + 3]) << 24);
    }

    public static long toBigEndianLong(byte[] buf, int offset)
    {
        return ((0xFF & (long) buf[offset + 0]) << 0)  |
               ((0xFF & (long) buf[offset + 1]) << 8)  |
               ((0xFF & (long) buf[offset + 2]) << 16) |
               ((0xFF & (long) buf[offset + 3]) << 24) |
               ((0xFF & (long) buf[offset + 4]) << 32) |
               ((0xFF & (long) buf[offset + 5]) << 40) |
               ((0xFF & (long) buf[offset + 6]) << 48) |
               ((0xFF & (long) buf[offset + 7]) << 56);
    }

    public static byte[] toLittleEndianBuf(int value)
    {
        byte[] ret = { ((byte) (value >>> 0)),
                       ((byte) (value >>> 8)),
                       ((byte) (value >>> 16)),
                       ((byte) (value >>> 24)) };
        return ret;
    }

    public static void putLongAsLittleEndian(byte[] buf, int index, long value)
    {
        for (int i = 0; i < 8; i++) {
            buf[index + i] = (byte) (value >>> i * 8);            
        }
    }
}
