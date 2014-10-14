/*
 * IP address as principal
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
package com.github.perlundq.yajsync.security;

import java.net.InetAddress;
import java.security.Principal;
import java.util.Objects;

public class AddressPrincipal implements Principal
{
    private final InetAddress _address;

    public AddressPrincipal(InetAddress address)
    {
        if (address == null) {
            throw new IllegalArgumentException();
        }
        _address = address;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other != null && getClass() == other.getClass()) {
            AddressPrincipal otherPrincipal = (AddressPrincipal) other;
            return _address.equals(otherPrincipal._address);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_address);
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), getName());
    }

    @Override
    public String getName()
    {
        return _address.getHostAddress();
    }

    public InetAddress inetAddress()
    {
        return _address;
    }
}
