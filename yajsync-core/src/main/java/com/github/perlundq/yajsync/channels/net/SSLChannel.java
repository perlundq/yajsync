/*
 * SSL/TLS implementation of ReadableByteChannel and WritableByteChannel
 *
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
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.Principal;

import javax.net.SocketFactory;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.github.perlundq.yajsync.util.Environment;

public class SSLChannel implements DuplexByteChannel
{
    private final InputStream _is;
    private final OutputStream _os;
    private final SSLSocket _sslSocket;

    public SSLChannel(SSLSocket sslSocket) throws IOException
    {
        assert Environment.hasAllocateDirectArray() ||
               !Environment.isAllocateDirect();
        _sslSocket = sslSocket;
        _is = _sslSocket.getInputStream();
        _os = _sslSocket.getOutputStream();
    }

    public static SSLChannel open(String address, int port) throws IOException
    {
        SocketFactory factory = SSLSocketFactory.getDefault();
        Socket sock = factory.createSocket(address, port);
        return new SSLChannel((SSLSocket) sock);
    }

    @Override
    public String toString()
    {
        return _sslSocket.toString();
    }

    @Override
    public void close() throws IOException
    {
        _sslSocket.close(); // will implicitly close _is and _os also
    }

    @Override
    public boolean isOpen()
    {
        return !_sslSocket.isClosed();
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        byte[] buf = src.array();
        int offset = src.arrayOffset() + src.position();
        int len = src.remaining();
        _os.write(buf, offset, len);
        src.position(src.position() + len);
        return len;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
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
    public InetAddress peerAddress()
    {
        InetAddress address = _sslSocket.getInetAddress();
        if (address == null) {
            throw new IllegalStateException(String.format(
                "unable to determine remote address of %s - not connected",
                _sslSocket));
        }
        return address;
    }

    @Override
    public boolean isPeerAuthenticated()
    {
        try {
            peerPrincipal();
            return true;
        } catch (IllegalStateException e) {
            return false;
        }
    }

    @Override
    public Principal peerPrincipal()
    {
        try {
            return _sslSocket.getSession().getPeerPrincipal();
        } catch (SSLPeerUnverifiedException e) {
            throw new IllegalStateException(e);
        }
    }
}
