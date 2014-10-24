/*
 * Rsync Channel with support for tagged rsync Messages sent from peer
 * and automatically prefetching of available amount
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
package com.github.perlundq.yajsync.channels;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.Util;

public class PrefetchedTaggedInputChannel extends TaggedInputChannel
{
    private static final int DEFAULT_BUF_SIZE = 8 * 1024; // TODO: make buffer size configurable
    private final ByteBuffer _buf;                                              // never flipped, never marked and its limit is never changed
    private int _readIndex = 0;

    public PrefetchedTaggedInputChannel(ReadableByteChannel sock,
                                        MessageHandler handler)
    {
        this(sock, handler, DEFAULT_BUF_SIZE);
    }

    public PrefetchedTaggedInputChannel(ReadableByteChannel sock,
                                        MessageHandler handler,
                                        int bufferSize)
    {
        super(sock, handler);
        if (Environment.isAllocateDirect()) {
            _buf = ByteBuffer.allocateDirect(bufferSize);
        } else {
            _buf = ByteBuffer.allocate(bufferSize);
        }
        _buf.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public String toString()
    {
        return String.format("buf=%s, readIndex=%d prefetched=%d, " +
                             "contents:%n\t%s",
                             _buf, _readIndex, numBytesPrefetched(),
                             Text.byteBufferToString(_buf,
                                                     _readIndex,
                                                     writeIndex()));
    }

    @Override
    public void get(ByteBuffer dst) throws ChannelException
    {
        ByteBuffer prefetched = nextReadableSlice(Math.min(numBytesPrefetched(),
                                                           dst.remaining()));
        dst.put(prefetched);
        super.get(dst);
    }

    @Override
    public ByteBuffer get(int numBytes) throws ChannelException
    {
        assert numBytes >= 0;
        assert numBytes <= _buf.capacity();
        ensureMinimumPrefetched(numBytes);
        ByteBuffer slice = nextReadableSlice(numBytes);
        assert slice.remaining() == numBytes;
        return slice;
    }

    @Override
    public byte getByte() throws ChannelException
    {
        ensureMinimumPrefetched(1);
        byte result = _buf.get(_readIndex);
        _readIndex += 1;
        return result;
    }

    @Override
    public char getChar() throws ChannelException
    {
        ensureMinimumPrefetched(2);
        char result = _buf.getChar(_readIndex);
        _readIndex += 2;
        return result;
    }

    @Override
    public int getInt() throws ChannelException
    {
        ensureMinimumPrefetched(4);
        int result = _buf.getInt(_readIndex);
        _readIndex += 4;
        return result;
    }

    @Override
    public int numBytesAvailable()
    {
        return super.numBytesAvailable() + numBytesPrefetched();
    }

    public int numBytesPrefetched()
    {
        assert _readIndex <= writeIndex();
        return writeIndex() - _readIndex;
    }

    private int writeIndex()
    {
        return _buf.position();
    }

    private ByteBuffer nextReadableSlice(int length)
    {
        assert length >= 0;
        assert length <= numBytesPrefetched();
        ByteBuffer slice = Util.slice(_buf, _readIndex, _readIndex + length);
        _readIndex += length;
        assert _readIndex <= _buf.limit();
        assert _readIndex <= writeIndex();
        assert slice.remaining() == length;
        return slice;
    }

    private ByteBuffer readableSlice()
    {
        return nextReadableSlice(numBytesPrefetched());
    }

    private void ensureSpaceFor(int numBytes)
    {
        assert numBytes >= 0;
        assert numBytes <= _buf.limit();
        if (_readIndex + numBytes > _buf.limit()) {
            ByteBuffer prefetched = readableSlice();
            assert _readIndex == writeIndex();
            prefetched.compact();
            _readIndex = 0;
            _buf.position(prefetched.position());
        }
    }

    private void ensureMinimumPrefetched(int numBytes) throws ChannelException
    {
        assert numBytes <= _buf.limit();
        ensureSpaceFor(numBytes);
        while (numBytesPrefetched() < numBytes) {
            readNextAvailable(_buf);
        }
        assert numBytesPrefetched() >= numBytes;
    }
}
