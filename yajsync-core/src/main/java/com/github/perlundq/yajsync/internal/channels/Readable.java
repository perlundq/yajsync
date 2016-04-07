/*
 * Interface for types support reading rsync basic data from peer
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

public interface Readable
{
    // NOTE: ByteBuffer.position() might very well be > 0, so calling rewind may lead to
    // unexpected behavior. Use mark and reset instead
    ByteBuffer get(int numBytes) throws ChannelException;
    void get(byte[] dst, int offset, int length) throws ChannelException;
    byte getByte() throws ChannelException;
    char getChar() throws ChannelException;
    int getInt() throws ChannelException;
    void skip(int numBytes) throws ChannelException;
}