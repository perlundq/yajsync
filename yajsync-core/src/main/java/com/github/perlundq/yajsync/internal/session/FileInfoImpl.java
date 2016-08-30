/*
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013-2016 Per Lundqvist
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

import java.util.Arrays;

import com.github.perlundq.yajsync.attr.FileInfo;
import com.github.perlundq.yajsync.attr.RsyncFileAttributes;
import com.github.perlundq.yajsync.internal.text.Text;

// NOTE: all internal FileInfo objects are instances of FileInfoImpl
class FileInfoImpl implements FileInfo
{
    // _pathNameOrNull may only be null internally in Receiver, any such
    // instance will never be exposed externally
    private final String _pathNameOrNull;
    private final byte[] _pathNameBytes;
    private final RsyncFileAttributes _attrs;

    FileInfoImpl(String pathNameOrNull, byte[] pathNameBytes,
                 RsyncFileAttributes attrs)
    {
        assert pathNameBytes != null;
        assert attrs != null;
        assert pathNameBytes.length > 0;
        assert pathNameBytes[0] != Text.ASCII_SLASH;
        assert !isDotDir(pathNameBytes) || attrs.isDirectory();
        assert pathNameBytes[pathNameBytes.length - 1] != Text.ASCII_SLASH;

        _pathNameOrNull = pathNameOrNull;
        _pathNameBytes = pathNameBytes;
        _attrs = attrs;
    }

    @Override
    public String toString()
    {
        String str = _pathNameOrNull == null
                ? "untransferrable " + Text.bytesToString(_pathNameBytes)
                : _pathNameOrNull;
        return String.format("%s (%s)", getClass().getSimpleName(), str);
    }

    @Override
    public boolean equals(Object obj)
    {
        // It is OK - FileInfo is not meant to be implemented by the API end
        // user and all our FileInfo implementing classes extends FileInfoImpl.
        if (obj instanceof FileInfoImpl) {
            FileInfoImpl other = (FileInfoImpl) obj;
            return Arrays.equals(_pathNameBytes, other._pathNameBytes);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(_pathNameBytes);
    }

    @Override
    public int compareTo(FileInfo otherFileInfo)
    {
        FileInfoImpl other = (FileInfoImpl) otherFileInfo;
        int result = compareUnixFileNamesBytes(_pathNameBytes,
                                               _attrs.isDirectory(),
                                               other._pathNameBytes,
                                               other._attrs.isDirectory());
        assert result != 0 || this.equals(other);
        return result;
    }

    @Override
    public RsyncFileAttributes attrs()
    {
        return _attrs;
    }

    @Override
    public String pathName()
    {
        return _pathNameOrNull;
    }

    boolean isDotDir()
    {
        return isDotDir(_pathNameBytes);
    }

    private static boolean isDotDir(byte[] bytes)
    {
        return bytes.length == 1 && bytes[0] == Text.ASCII_DOT;
    }

    /*
     * sort . (always a dir) before anything else
     * sort files before dirs
     * compare dirs using a trailing slash
     */
    private static int compareUnixFileNamesBytes(byte[] leftBytes,
                                                 boolean isLeftDir,
                                                 byte[] rightBytes,
                                                 boolean isRightDir)
    {
        if (isDotDir(leftBytes)) {
            if (isDotDir(rightBytes)) {
                return 0;
            }
            return -1;
        } else if (isDotDir(rightBytes)) {
            return 1;
        }

        if (isLeftDir != isRightDir) {
            if (isLeftDir) {
                return 1;
            }
            return -1;
        }

        int i = 0;
        for (; i < leftBytes.length && i < rightBytes.length; i++) {
            int diff = leftBytes[i] - rightBytes[i];
            if (diff != 0) {
                return diff;
            }
        }
        // one or both are at the end
        // one or both are a substring of the other
        // either both are directories or none is
        boolean isLeftAtEnd = i == leftBytes.length;
        boolean isRightAtEnd = i == rightBytes.length;

        if (isLeftDir) { // && isRightDir
            if (isLeftAtEnd && isRightAtEnd) {
                return 0;
            } else if (isLeftAtEnd) {
                return Text.ASCII_SLASH - rightBytes[i];
            } else if (isRightAtEnd) {
                return leftBytes[i] - Text.ASCII_SLASH;
            }
        }

        if (isLeftAtEnd == isRightAtEnd) {
            return 0;
        } else if (isLeftAtEnd) {
            return -1;
        }
        return 1;
    }
}
