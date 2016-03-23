/*
 * Rsync channel tagged message information (see TaggedInputChannel
 * and TaggedOutputChannel)
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
import java.util.Objects;
import java.util.logging.Level;

import com.github.perlundq.yajsync.text.Text;

public class Message
{
    private final MessageHeader _header;
    private final ByteBuffer _payload;

    /**
     * @throws IllegalArgumentException
     * @throws IllegalStateException
     */
    public Message(MessageCode code, ByteBuffer payload)
    {
        this(new MessageHeader(code, payload.remaining()), payload);
    }

    /**
     * @throws IllegalArgumentException if type is IO_ERROR or NO_SEND and
     *         length of message header is not 4 bytes
     * @throws IllegalStateException if header is of an unsupported message type
     */
    public Message(MessageHeader header, ByteBuffer payload)
    {
        switch (header.messageType()) {
        case IO_ERROR:
        case NO_SEND:
            if (header.length() != 4) {
                throw new IllegalArgumentException(String.format(
                    "received tag %s with an illegal number of bytes, got %d," +
                    " expected %d", header.messageType(), header.length(), 4));
            }
            break;
        case DATA:
        case INFO:
        case ERROR:
        case ERROR_XFER:
        case WARNING:
        case LOG:
            break;
        default:
            throw new IllegalStateException("TODO: (not yet implemented) " +
                                            "missing case statement for " +
                                            header.messageType());
        }
        _header = header;
        _payload = payload;
    }

    @Override
    public String toString()
    {
        return String.format("%s %s %s %s",
                             getClass().getSimpleName(), _header, _payload,
                             Text.byteBufferToString(_payload));
    }

    /**
     * Note: Message.payload() changes state when being read
     */
    @Override
    public boolean equals(Object obj)
    {
        if (obj != null && this.getClass() == obj.getClass()) {
            Message other = (Message) obj;
            if (this.header().equals(other.header())) {
                return _payload.equals(other._payload);
            }
        }
        return false;
    }

    /**
     * Note: Message.payload() changes state when being read
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(_header, _payload);
    }

    public MessageHeader header()
    {
        return _header;
    }

    public ByteBuffer payload()
    {
        return _payload;
    }

    public boolean isText()
    {
        switch (_header.messageType()) {
        case LOG:
        case INFO:
        case WARNING:
        case ERROR_XFER:
        case ERROR:
            return true;
        default:
            return false;
        }
    }

    public Level logLevelOrNull()
    {
        assert isText();
        switch (_header.messageType()) {
        case LOG:
            return Level.FINE;
        case INFO:
            return Level.INFO;
        case WARNING:
            return Level.WARNING;
        case ERROR_XFER: // fall through
        case ERROR:
            return Level.SEVERE;
        default:
            return null;
        }
    }
}
