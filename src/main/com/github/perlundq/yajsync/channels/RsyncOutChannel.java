/*
 * Rsync Channel with support for sending tagged rsync Messages to
 * peer + file list index encoding
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

import java.nio.channels.WritableByteChannel;

public class RsyncOutChannel extends TaggedOutputChannel implements IndexEncoder
{
    private final IndexEncoder _indexEncoder;

    public RsyncOutChannel(WritableByteChannel sock)
    {
        super(sock);
        _indexEncoder = new IndexEncoderImpl(this);
    }

    public RsyncOutChannel(WritableByteChannel sock, int bufferSize)
    {
        super(sock, bufferSize);
        _indexEncoder = new IndexEncoderImpl(this);
    }

    @Override
    public void encodeIndex(int index) throws ChannelException
    {
        _indexEncoder.encodeIndex(index);
    }
}
