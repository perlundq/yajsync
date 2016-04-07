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
package com.github.perlundq.yajsync.net;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;

public class StandardServerChannel implements ServerChannel
{
    private final ServerSocketChannel _sock;
    private final int _timeout;

    public StandardServerChannel(ServerSocketChannel sock, int timeout)
    {
        _sock = sock;
        _timeout = timeout;
    }

    @Override
    public void close() throws IOException
    {
        _sock.close();
    }

    @Override
    public StandardSocketChannel accept() throws IOException
    {
        return new StandardSocketChannel(_sock.accept(), _timeout);
    }
}
