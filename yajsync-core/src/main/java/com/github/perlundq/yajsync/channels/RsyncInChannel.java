/*
 * Rsync Channel with support for tagged rsync Messages sent from peer
 * and automatically prefetching of available amount + file list index
 * decoding
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

import java.nio.channels.ReadableByteChannel;

public class RsyncInChannel extends PrefetchedTaggedInputChannel
                            implements IndexDecoder
{
    private final IndexDecoder _indexDecoder;

    public RsyncInChannel(ReadableByteChannel sock, MessageHandler handler)
    {
        super(sock, handler);
        _indexDecoder = new IndexDecoderImpl(this);

    }

    public RsyncInChannel(ReadableByteChannel sock, MessageHandler handler,
                          int bufferSize)
    {
        super(sock, handler, bufferSize);
        _indexDecoder = new IndexDecoderImpl(this);

    }

    @Override
    public int decodeIndex() throws ChannelException
    {
        return _indexDecoder.decodeIndex();
    }
}
