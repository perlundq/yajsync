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
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;

public class StandardServerChannelFactory implements ServerChannelFactory
{
    private boolean _isReuseAddress;

    @Override
    public ServerChannelFactory setReuseAddress(boolean isReuseAddress)
    {
        _isReuseAddress = isReuseAddress;
        return this;
    }

    @Override
    public ServerChannel open(InetAddress address, int port) throws IOException
    {
        ServerSocketChannel sock = ServerSocketChannel.open();
        try {
            if (_isReuseAddress) {
                sock.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            }
            InetSocketAddress socketAddress =
                new InetSocketAddress(address, port);
            sock.bind(socketAddress);
            return new StandardServerChannel(sock);
        } catch (Throwable t) {
            try {
                if (sock.isOpen()) {
                    sock.close();
                }
            } catch (Throwable tt) {
                t.addSuppressed(tt);
            }
            throw t;
        }
    }
}
