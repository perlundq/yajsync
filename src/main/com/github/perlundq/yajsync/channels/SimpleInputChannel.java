/*
 * Rsync Channel with support for reading basic data from peer
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
package com.github.perlundq.yajsync.channels;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ReadableByteChannel;

import com.github.perlundq.yajsync.util.RuntimeInterruptException;

public class SimpleInputChannel implements Readable
{
    private static final int DEFAULT_BUF_SIZE = 1024;
    private final ReadableByteChannel _sourceChannel;
    private final ByteBuffer _byteBuf = ByteBuffer.allocateDirect(1);
    private final ByteBuffer _charBuf = ByteBuffer.allocateDirect(2).
                                            order(ByteOrder.LITTLE_ENDIAN);
    private final ByteBuffer _intBuf = ByteBuffer.allocateDirect(4).
                                            order(ByteOrder.LITTLE_ENDIAN);
    private long _numBytesRead;

    public SimpleInputChannel(ReadableByteChannel sock)
    {
        assert sock != null;
        _sourceChannel = sock;
    }
    
    @Override
    public byte getByte() throws ChannelException
    {
        _byteBuf.clear();
        get(_byteBuf);
        _byteBuf.flip();
        return _byteBuf.get();
    }

    @Override
    public char getChar() throws ChannelException
    {
        _charBuf.clear();
        get(_charBuf);
        _charBuf.flip();
        return _charBuf.getChar();
    }

    @Override
    public int getInt() throws ChannelException
    {
        _intBuf.clear();
        get(_intBuf);
        _intBuf.flip();
        return _intBuf.getInt();
    }
     
    @Override
    public ByteBuffer get(int numBytes) throws ChannelException
    {
        ByteBuffer result = ByteBuffer.allocate(numBytes);
        get(result);
        result.flip();
        return result;
    }
    
    @Override
    public void get(byte[] dst, int offset, int length) throws ChannelException
    {
        get(ByteBuffer.wrap(dst, offset, length));
    }

    @Override
    public void skip(int numBytes) throws ChannelException
    {
        assert numBytes >= 0;
        int numBytesSkipped = 0;
        while (numBytesSkipped < numBytes) {
            int chunkSize = Math.min(numBytes - numBytesSkipped,
                                     DEFAULT_BUF_SIZE);
            get(chunkSize); // ignore result
            numBytesSkipped += chunkSize;
        }
    }

    public long numBytesRead()
    {
        return _numBytesRead;
    }

    protected void get(ByteBuffer dst) throws ChannelException
    {
        try {
            while (dst.hasRemaining()) {
                int count = _sourceChannel.read(dst);
                if (count <= 0) {
                    throw new ChannelEOFException(String.format(
                        "channel read unexpectedly returned %d (EOF)", count));
                }
                _numBytesRead += count;
            }
        } catch (EOFException e) {
            throw new ChannelEOFException(e);
        } catch (ClosedByInterruptException e) {
            throw new RuntimeInterruptException(e);
        } catch (IOException e) {
            throw new ChannelException(e);
        }
    }
}
