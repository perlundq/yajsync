/*
 * Copyright (C) 2014 Per Lundqvist
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
package com.github.perlundq.yajsync.channels.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.Principal;

public class StandardSocketChannel implements DuplexByteChannel
{
    private final SocketChannel _sock;

    public StandardSocketChannel(SocketChannel sock)
    {
        _sock = sock;
    }

    @Override
    public String toString()
    {
        return _sock.toString();
    }

    @Override
    public boolean isOpen()
    {
        return _sock.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        _sock.close();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        return _sock.read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        return _sock.write(src);
    }

    @Override
    public InetAddress peerAddress()
    {
        try {
            InetSocketAddress socketAddress =
                (InetSocketAddress) _sock.getRemoteAddress();
            if (socketAddress == null) {
                throw new IllegalStateException(String.format(
                    "unable to determine remote address of %s - not connected",
                    _sock));
            }
            InetAddress addrOrNull = socketAddress.getAddress();
            if (addrOrNull == null) {
                throw new IllegalStateException(String.format(
                    "unable to determine address of %s - unresolved",
                    socketAddress));
            }
            return addrOrNull;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean isPeerAuthenticated()
    {
        return false;
    }

    @Override
    public Principal peerPrincipal()
    {
        throw new UnsupportedOperationException();
    }
}
