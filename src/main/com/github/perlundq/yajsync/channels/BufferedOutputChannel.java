/*
 * Rsync channel with support for sending and buffer basic data to
 * peer
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.WritableByteChannel;

import com.github.perlundq.yajsync.util.Consts;
import com.github.perlundq.yajsync.util.RuntimeInterruptException;
import com.github.perlundq.yajsync.util.Util;

public class BufferedOutputChannel implements Bufferable
{
    private static final int DEFAULT_BUF_SIZE = 8 * 1024;
    private final WritableByteChannel _sinkChannel;
    protected final ByteBuffer _buffer;
    private long _numBytesWritten;
 
    public BufferedOutputChannel(WritableByteChannel sock)
    {
        this(sock, DEFAULT_BUF_SIZE);
    }

    public BufferedOutputChannel(WritableByteChannel sock, int bufferSize)
    {
        _sinkChannel = sock;
        _buffer = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
    }
    
    public void send(ByteBuffer buf) throws ChannelException
    {
        try {
            while (buf.hasRemaining()) {
                int count = _sinkChannel.write(buf);
                if (count <= 0) {
                    throw new ChannelEOFException(String.format(
                        "channel write unexpectedly returned %d (EOF)", count));
                }
                _numBytesWritten += count;
            }
        } catch (ClosedByInterruptException e) {
            throw new RuntimeInterruptException(e);
        } catch (IOException e) {
            throw new ChannelException(e);
        }
    }
    
    @Override
    public void flush() throws ChannelException
    {
        if (_buffer.position() > 0) {
            _buffer.flip();
            send(_buffer);
            _buffer.clear();
        }
    }

    @Override
    public void put(ByteBuffer src) throws ChannelException
    {
        while (src.hasRemaining()) {
            int l = Math.min(src.remaining(), _buffer.remaining());
            if (l == 0) {
                flush();
            } else {
                ByteBuffer slice = Util.slice(src,
                                              src.position(),
                                              src.position() + l);
                _buffer.put(slice);
                src.position(slice.position());
            }
        }
    }

    @Override
    public void put(byte[] src, int offset, int length)
        throws ChannelException
    {
        put(ByteBuffer.wrap(src, offset, length));
    }

    @Override
    public void putByte(byte b) throws ChannelException
    {
        if (_buffer.remaining() < Consts.SIZE_BYTE) {
            flush();
        }
        _buffer.put(b);
    }

    @Override
    public void putChar(char c) throws ChannelException
    {
        if (_buffer.remaining() < Consts.SIZE_CHAR) {
            flush();
        }
        _buffer.putChar(c);
    }

    @Override
    public void putInt(int i) throws ChannelException
    {
        if (_buffer.remaining() < Consts.SIZE_INT) {
            flush();
        }
        _buffer.putInt(i);
    }
    
    public long numBytesWritten()
    {
        return _numBytesWritten + _buffer.position();
    }
}
