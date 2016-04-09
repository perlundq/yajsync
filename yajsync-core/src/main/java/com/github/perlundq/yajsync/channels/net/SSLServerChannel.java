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

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

public class SSLServerChannel implements ServerChannel
{
    private final SSLServerSocket _sslSocket;
    private final int _timeout;

    public SSLServerChannel(SSLServerSocket sock, int timeout)
    {
        _sslSocket = sock;
        _timeout = timeout;
    }

    @Override
    public void close() throws IOException
    {
        _sslSocket.close();
    }

    @Override
    public SSLChannel accept() throws IOException
    {
        return new SSLChannel((SSLSocket) _sslSocket.accept(), _timeout);
    }
}
