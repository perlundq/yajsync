/*
 * Copyright (C) 2014 Per Lundqvist
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
package com.github.perlundq.yajsync.test;

import java.nio.ByteBuffer;

import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.channels.Readable;
import com.github.perlundq.yajsync.util.Util;

public class ReadableByteBuffer implements Readable
{
    private final ByteBuffer _buf;

    public ReadableByteBuffer(ByteBuffer buf) {
        _buf = buf;
    }

    @Override
    public void get(byte[] dst, int offset, int length)
    {
        _buf.get(dst, offset, length);
    }

    @Override
    public ByteBuffer get(int numBytes)
    {
        return Util.slice(_buf, 0, numBytes);
    }

    @Override
    public byte getByte()
    {
        return _buf.get();
    }

    @Override
    public char getChar()
    {
        return _buf.getChar();
    }

    @Override
    public int getInt()
    {
        return _buf.getInt();
    }

    @Override
    public void skip(int numBytes)
    {
        _buf.position(_buf.position() + numBytes);
    }
}