/*
 * Copyright (C) 2014-2016 Per Lundqvist
 * Copyright (C) 2015-2016 Florian Sager
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
package com.github.perlundq.yajsync.attr;

import java.util.Objects;

import com.github.perlundq.yajsync.internal.util.Environment;

public final class User
{
    public static final int ID_MAX = 65535;
    private static final int ID_NOBODY = ID_MAX - 1;
    private static final int MAX_NAME_LENGTH = 255;

    public static final User ROOT = new User("root", 0);
    public static final User NOBODY = new User("nobody", ID_NOBODY);
    public static final User JVM_USER = new User(Environment.getUserName(),
                                                  Environment.getUserId());

    private final String _name;
    private final int _id;

    public User(String name, int uid)
    {
        if (name == null) {
            throw new IllegalArgumentException();
        }
        if (name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException();
        }
        if (uid < 0 || uid > ID_MAX) {
            throw new IllegalArgumentException();
        }
        _name = name;
        _id = uid;
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s, %d)",
                             getClass().getSimpleName(), _name, _id);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        } else if (other != null && getClass() == other.getClass()) {
            User otherUser = (User) other;
            return _id == otherUser._id && _name.equals(otherUser._name);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_name, _id);
    }

    public String name()
    {
        return _name;
    }

    public int id()
    {
        return _id;
    }
}
