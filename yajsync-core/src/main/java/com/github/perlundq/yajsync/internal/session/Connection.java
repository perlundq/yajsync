/*
 * Common connection related routines for Sender and Generator
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
package com.github.perlundq.yajsync.internal.session;

import com.github.perlundq.yajsync.RsyncProtocolException;
import com.github.perlundq.yajsync.internal.channels.ChannelException;
import com.github.perlundq.yajsync.internal.channels.Readable;
import com.github.perlundq.yajsync.internal.channels.Writable;

final class Connection
{
    private Connection() {}

    public static Checksum.Header receiveChecksumHeader(Readable conn)
        throws ChannelException, RsyncProtocolException
    {
        try {
            int chunkCount = conn.getInt();
            int blockLength = conn.getInt();
            int digestLength = conn.getInt();
            int remainder = conn.getInt();
            return new Checksum.Header(chunkCount, blockLength, remainder,
                                       digestLength);
        } catch (IllegalArgumentException e) {
            throw new RsyncProtocolException(e);
        }
    }

    public static void sendChecksumHeader(Writable conn, Checksum.Header header)
        throws ChannelException
    {
        conn.putInt(header.chunkCount());
        conn.putInt(header.blockLength());
        conn.putInt(header.digestLength());
        conn.putInt(header.remainder());
    }
}
