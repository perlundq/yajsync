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

public class AutoFlushableRsyncDuplexChannel extends AutoFlushableDuplexChannel
                                             implements Taggable, IndexDecoder,
                                                        IndexEncoder
{
    private final RsyncInChannel _inChannel;
    private final RsyncOutChannel _outChannel;

    public AutoFlushableRsyncDuplexChannel(RsyncInChannel inChannel,
                                           RsyncOutChannel outChannel)
    {
        super(inChannel, outChannel);
        _inChannel = inChannel;
        _outChannel = outChannel;
    }

    @Override
    public void flush() throws ChannelException
    {
        if (_inChannel.numBytesAvailable() == 0) {
            super.flush();
        }
    }

    @Override
    public void putMessage(Message message) throws ChannelException
    {
        _outChannel.putMessage(message);
    }

    @Override
    public void encodeIndex(int index) throws ChannelException
    {
        _outChannel.encodeIndex(index);
    }

    @Override
    public int decodeIndex() throws ChannelException
    {
        flush();
        return _inChannel.decodeIndex();
    }

    public int numBytesAvailable()
    {
        return _inChannel.numBytesAvailable();
    }

    public long numBytesRead()
    {
        return _inChannel.numBytesRead();
    }

    public long numBytesWritten()
    {
        return _outChannel.numBytesWritten();
    }

    public void close() throws ChannelException
    {
        try {
            _inChannel.close();
        } finally {
            _outChannel.close();
        }
    }
}
