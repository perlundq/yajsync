/*
 * Rsync file list index encoding routine
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
package com.github.perlundq.yajsync.internal.channels;

import com.github.perlundq.yajsync.internal.session.Filelist;

public class IndexEncoderImpl implements IndexEncoder
{
    private final Writable _dst;
    private int _prevNegativeWriteIndex = 1;
    private int _prevPositiveWriteIndex = -1;

    public IndexEncoderImpl(Writable dst)
    {
        _dst = dst;
    }

    //  A diff of 1 - 253 is sent as a one-byte diff; a diff of 254 - 32767
    //  or 0 is sent as a 0xFE + a two-byte diff; otherwise send 0xFE
    //  and all 4 bytes of the (non-negative) num with the high-bit set.
    @Override
    public void encodeIndex(int index) throws ChannelException
    {
        if (index == Filelist.DONE) {
            _dst.putByte((byte) 0);
            return;
        }

        int indexPositive;
        int diff;
        if (index >= 0) {
            indexPositive = index;
            diff = indexPositive - _prevPositiveWriteIndex;
            _prevPositiveWriteIndex = indexPositive;
        } else {
            indexPositive = -index;
            diff = indexPositive - _prevNegativeWriteIndex;
            _prevNegativeWriteIndex = indexPositive;
            _dst.putByte((byte) 0xFF);
        }

        if (diff < 0xFE && diff > 0) {
            _dst.putByte((byte) diff);
        } else if (diff < 0 || diff > 0x7FFF) {
            _dst.putByte((byte) 0xFE);
            _dst.putByte((byte) ((indexPositive >> 24) | 0x80));
            _dst.putByte((byte) indexPositive);
            _dst.putByte((byte) (indexPositive >> 8));
            _dst.putByte((byte) (indexPositive >> 16));
        } else {
            _dst.putByte((byte) 0xFE);
            _dst.putByte((byte) (diff >> 8));
            _dst.putByte((byte) diff);
        }
    }
}
