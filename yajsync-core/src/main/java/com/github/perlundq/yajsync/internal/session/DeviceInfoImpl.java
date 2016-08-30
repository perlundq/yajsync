/*
 * Copyright (C) 2016 Per Lundqvist
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

import com.github.perlundq.yajsync.attr.RsyncFileAttributes;
import com.github.perlundq.yajsync.attr.DeviceInfo;

class DeviceInfoImpl extends FileInfoImpl implements DeviceInfo
{
    private final int _major;
    private final int _minor;

    DeviceInfoImpl(String pathName, byte[] pathNameBytes,
                   RsyncFileAttributes attrs, int major, int minor)
    {
        super(pathName, pathNameBytes, attrs);
        assert attrs.isBlockDevice() || attrs.isCharacterDevice() ||
               attrs.isFifo() || attrs.isSocket();
        assert major >= 0;
        assert minor >= 0;
        _major = major;
        _minor = minor;
    }

    @Override
    public int major()
    {
        return _major;
    }

    @Override
    public int minor()
    {
        return _minor;
    }
}
