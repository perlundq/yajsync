/*
 * Rsync network protocol version type
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
package com.github.perlundq.yajsync.session;

import java.util.Objects;

public final class ProtocolVersion implements Comparable<ProtocolVersion>
{
    private final int _major;
    private final int _minor;

    public ProtocolVersion(int major, int minor)
    {
        _major = major;
        _minor = minor;
    }

    @Override
    public String toString()
    {
        return String.format("%d.%d", _major, _minor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_major, _minor);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o != null && getClass() == o.getClass()) {
            return compareTo((ProtocolVersion) o) == 0;
        }
        return false;
    }

    @Override
    public int compareTo(ProtocolVersion other)
    {
        int res = _major - other._major;
        if (res == 0) {
            return _minor - other._minor;
        }
        return res;
    }

    public int major()
    {
        return _major;
    }

    public int minor()
    {
        return _minor;
    }
}
