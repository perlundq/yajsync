/*
 * Rsync Channel with support for handling tagged rsync Messages sent
 * from peer
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013, 2014, 2016 Per Lundqvist
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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.session.RsyncProtocolException;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.Util;

public class TaggedInputChannel extends SimpleInputChannel
{
    private static final Logger _log =
        Logger.getLogger(TaggedInputChannel.class.getName());

    private final SimpleInputChannel _inputChannel;
    private final MessageHandler _msgHandler;
    private int _readAmountAvailable = 0;

    public TaggedInputChannel(ReadableByteChannel sock, MessageHandler handler)
    {
        super(sock);
        _inputChannel = new SimpleInputChannel(sock);
        _msgHandler = handler;
    }

    @Override
    protected void get(ByteBuffer dst) throws ChannelException
    {
        while (dst.hasRemaining()) {
            readNextAvailable(dst);
        }
    }

    public int numBytesAvailable()
    {
        return _readAmountAvailable;
    }

    /**
     * @throws RsyncProtocolException if peer sends an invalid message
     */
    protected void readNextAvailable(ByteBuffer dst) throws ChannelException
    {
        while (_readAmountAvailable == 0) {
            _readAmountAvailable = readNextMessage();
        }
        int chunkLength = Math.min(_readAmountAvailable, dst.remaining());
        ByteBuffer slice = Util.slice(dst,
                                      dst.position(),
                                      dst.position() + chunkLength);
        super.get(slice);
        if (_log.isLoggable(Level.FINEST)) {
            ByteBuffer tmp = Util.slice(dst,
                                        dst.position(),
                                        dst.position() + Math.min(chunkLength,
                                                                  64));
            _log.finest(Text.byteBufferToString(tmp));
        }
        dst.position(slice.position());
        _readAmountAvailable -= chunkLength;
    }

    @Override
    public long numBytesRead()
    {
        return super.numBytesRead() + _inputChannel.numBytesRead();
    }

    /**
     * @throws RsyncProtocolException
     */
    private int readNextMessage() throws ChannelException
    {
        try {
            // throws IllegalArgumentException
            MessageHeader hdr = MessageHeader.fromTag(_inputChannel.getInt());
            if (hdr.messageType() == MessageCode.DATA) {
                if (_log.isLoggable(Level.FINER)) {
                    _log.finer("< " + hdr);
                }
                return hdr.length();
            }
            ByteBuffer payload = _inputChannel.get(hdr.length()).
                                               order(ByteOrder.LITTLE_ENDIAN);
            // throws IllegalArgumentException, IllegalStateException
            Message message = new Message(hdr, payload);
            if (_log.isLoggable(Level.FINER)) {
                _log.finer("< " + message);
            }
            _msgHandler.handleMessage(message);
            return 0;
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw new RsyncProtocolException(e);
        }
    }
}
