/*
 * Caching of previous sent file meta data for minimising
 * communication between Sender and Receiver
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
package com.github.perlundq.yajsync.internal.session;

import com.github.perlundq.yajsync.attr.Group;
import com.github.perlundq.yajsync.attr.User;

class FileInfoCache
{
    private int _prevMajor = -1;
    private int _prevMode = -1;
    private byte[] _prevFileName = {};
    private long _prevLastModified = 0;
    private User _prevUser;
    private Group _prevGroup;

    public FileInfoCache() {}

    public int getPrevMode()
    {
        return _prevMode;
    }

    public void setPrevMode(int prevMode)
    {
        _prevMode = prevMode;
    }

    public byte[] getPrevFileNameBytes()
    {
        return _prevFileName;
    }

    public void setPrevFileNameBytes(byte[] prevFileName)
    {
        _prevFileName = prevFileName;
    }

    public long getPrevLastModified()
    {
        return _prevLastModified;
    }

    public void setPrevLastModified(long prevLastModified)
    {
        _prevLastModified = prevLastModified;
    }

    public User getPrevUserOrNull()
    {
        return _prevUser;
    }

    public void setPrevUser(User user)
    {
        _prevUser = user;
    }

    public Group getPrevGroupOrNull()
    {
        return _prevGroup;
    }

    public void setPrevGroup(Group group)
    {
        _prevGroup = group;
    }

    public void setPrevMajor(int major)
    {
        _prevMajor = major;
    }

    public int getPrevMajor()
    {
        return _prevMajor;
    }
}
