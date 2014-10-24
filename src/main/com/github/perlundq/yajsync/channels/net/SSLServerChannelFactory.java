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

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

public class SSLServerChannelFactory implements ServerChannelFactory
{
    private final SSLServerSocketFactory _factory;

    private boolean _isWantClientAuth;
    private boolean _isReuseAddress;
    private int _backlog = 128;

    public SSLServerChannelFactory()
    {
        _factory = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
    }

    @Override
    public ServerChannelFactory setReuseAddress(boolean isReuseAddress)
    {
        _isReuseAddress = isReuseAddress;
        return this;
    }

    public ServerChannelFactory setWantClientAuth(boolean isWantClientAuth)
    {
        _isWantClientAuth = isWantClientAuth;
        return this;
    }

    public ServerChannelFactory setBacklog(int backlog)
    {
        _backlog = backlog;
        return this;
    }

    @Override
    public ServerChannel open(InetAddress address, int port) throws IOException
    {
        SSLServerSocket sock =
            (SSLServerSocket) _factory.createServerSocket(port,
                                                          _backlog, address);
        try {
            sock.setReuseAddress(_isReuseAddress);
            sock.setWantClientAuth(_isWantClientAuth);
            return new SSLServerChannel(sock);
        } catch (Throwable t) {
            if (!sock.isClosed()) {
                try {
                    sock.close();
                } catch (Throwable tt) {
                    t.addSuppressed(tt);
                }
            }
            throw t;
        }
    }
}
