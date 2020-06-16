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

import java.nio.ByteBuffer;
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
    private final ByteBuffer _pathNameBytes;
    private final RsyncFileAttributes _attrs;

    FileInfoImpl(String pathNameOrNull, byte[] pathNameBytes,
                    RsyncFileAttributes attrs)
    {
        this(pathNameOrNull, ByteBuffer.wrap( pathNameBytes ), attrs );
    }
    
    FileInfoImpl(String pathNameOrNull, ByteBuffer pathNameBytes,
                 RsyncFileAttributes attrs)
    {
        assert pathNameBytes != null;
        assert attrs != null;
        assert pathNameBytes.remaining() > 0;
        assert pathNameBytes.position() == 0;
        assert pathNameBytes.get( 0 ) != Text.ASCII_SLASH;
        assert !isDotDir(pathNameBytes) || attrs.isDirectory();
        assert pathNameBytes.get(pathNameBytes.limit() - 1) != Text.ASCII_SLASH;

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
            return _pathNameBytes.equals( other._pathNameBytes );
        } else {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return _pathNameBytes.hashCode();
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

    private static boolean isDotDir(ByteBuffer bytes)
    {
        return bytes.remaining() == 1 && bytes.get( bytes.position() ) == Text.ASCII_DOT;
    }

    private static int cmp(byte a, byte b)
    {
        return (0xFF & a) - (0xFF & b);
    }

    /*
     * sort . (always a dir) before anything else
     * sort files before dirs
     * compare dirs using a trailing slash
     */
    private static int compareUnixFileNamesBytes(ByteBuffer leftBytes,
                                                 boolean isLeftDir,
                                                 ByteBuffer rightBytes,
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
        for (; i < leftBytes.limit() && i < rightBytes.limit(); i++) {
            int diff = cmp(leftBytes.get(i), rightBytes.get(i));
            if (diff != 0) {
                return diff;
            }
        }
        // one or both are at the end
        // one or both are a substring of the other
        // either both are directories or none is
        boolean isLeftAtEnd = i == leftBytes.limit();
        boolean isRightAtEnd = i == rightBytes.limit();

        if (isLeftDir) { // && isRightDir
            if (isLeftAtEnd && isRightAtEnd) {
                return 0;
            } else if (isLeftAtEnd) {
                return cmp(Text.ASCII_SLASH, rightBytes.get(i));
            } else if (isRightAtEnd) {
                return cmp(leftBytes.get(i), Text.ASCII_SLASH);
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
