/*
 * Automatically flush output channel for read operations
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
package com.github.perlundq.yajsync.internal.channels;

import java.nio.ByteBuffer;

public class AutoFlushableDuplexChannel extends BufferedDuplexChannel
{
    public AutoFlushableDuplexChannel(Readable readable, Bufferable writable)
    {
        super(readable, writable);
    }

    @Override
    public ByteBuffer get(int numBytes) throws ChannelException
    {
        flush();
        return super.get(numBytes);
    }

    @Override
    public byte getByte() throws ChannelException
    {
        flush();
        return super.getByte();
    }

    @Override
    public char getChar() throws ChannelException
    {
        flush();
        return super.getChar();
    }

    @Override
    public int getInt() throws ChannelException
    {
        flush();
        return super.getInt();
    }
}
