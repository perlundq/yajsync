/*
 * Copyright (C) 2013-2015 Per Lundqvist
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
package com.github.perlundq.yajsync.ui;

import java.util.Objects;

import com.github.perlundq.yajsync.util.Consts;
import com.github.perlundq.yajsync.util.Environment;

final class ConnInfo
{
    public static final int PORT_MIN = 1;
    public static final int PORT_MAX = 65535;
    private final int _portNumber;
    private final String _userName;
    private final String _address;

    private ConnInfo(Builder builder)
    {
        _userName = builder._userName;
        _address = builder._address;
        _portNumber = builder._portNumber;
    }

    @Override
    public String toString()
    {
        return String.format("rsync://%s%s:%d",
                             _userName.isEmpty() ? "" : _userName + "@",
                             _address, _portNumber);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj != null && getClass() == obj.getClass()) {
            ConnInfo other = (ConnInfo) obj;
            return _userName.equals(other._userName) &&
                   _address.equals(other._address) &&
                   _portNumber == other._portNumber;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_address, _userName, _portNumber);
    }

    public String userName()
    {
        return _userName;
    }

    public String address()
    {
        return _address;
    }

    public int portNumber()
    {
        return _portNumber;
    }

    public static boolean isValidPortNumber(int portNumber)
    {
        return portNumber >= PORT_MIN && portNumber <= PORT_MAX;
    }

    public static class Builder
    {
        private final String _address;
        private String _userName = Environment.getUserName();
        private int _portNumber = Consts.DEFAULT_LISTEN_PORT;

        public Builder(String address) throws IllegalUrlException
        {
            assert address != null;
            if (address.isEmpty()) {
                throw new IllegalUrlException("address is empty");
            }
            _address = address;
        }

        public Builder userName(String userName)
        {
            assert userName != null;
            _userName = userName;
            return this;
        }

        public Builder portNumber(int portNumber) throws IllegalUrlException
        {
            if (!isValidPortNumber(portNumber)) {
                throw new IllegalUrlException(String.format(
                        "illegal port %d - must be within the range [%d, %d]",
                        portNumber, PORT_MIN, PORT_MAX));
            }
            _portNumber = portNumber;
            return this;
        }

        public ConnInfo build()
        {
            return new ConnInfo(this);
        }
    }
}
