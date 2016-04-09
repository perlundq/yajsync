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
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.Principal;

import com.github.perlundq.yajsync.util.Environment;

public class StandardSocketChannel implements DuplexByteChannel
{
    private final InputStream _is;
    private final SocketChannel _socketChannel;
    private final int _timeout;

    public StandardSocketChannel(SocketChannel socketChannel, int timeout) throws IOException
    {
        if (timeout > 0) {
            assert Environment.hasAllocateDirectArray() ||
            !Environment.isAllocateDirect();
        }
        _socketChannel = socketChannel;
        _timeout = timeout;
        _socketChannel.socket().setSoTimeout(timeout * 1000);
        _is = _socketChannel.socket().getInputStream();
    }

    public static StandardSocketChannel open(String address, int port, int contimeout, int timeout) throws IOException
    {
        InetSocketAddress socketAddress = new InetSocketAddress(address, port);
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.socket().connect(socketAddress, contimeout * 1000);
        return new StandardSocketChannel(socketChannel, timeout);
    }

    @Override
    public String toString()
    {
        return _socketChannel.toString();
    }

    @Override
    public boolean isOpen()
    {
        return _socketChannel.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        _socketChannel.close();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        if (_timeout == 0) {
            return _socketChannel.read(dst);
        }

        byte[] buf = dst.array();
        int offset = dst.arrayOffset() + dst.position();
        int len = dst.remaining();
        int n = _is.read(buf, offset, len);
        if (n != -1) {
            dst.position(dst.position() + n);
        }
        return n;
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        return _socketChannel.write(src);
    }

    @Override
    public InetAddress peerAddress()
    {
        try {
            InetSocketAddress socketAddress =
                (InetSocketAddress) _socketChannel.getRemoteAddress();
            if (socketAddress == null) {
                throw new IllegalStateException(String.format(
                    "unable to determine remote address of %s - not connected",
                    _socketChannel));
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
