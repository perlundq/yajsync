/*
 * Rsync file attributes
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
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
package com.github.perlundq.yajsync.attr;

import java.util.Objects;

import com.github.perlundq.yajsync.internal.util.FileOps;

public class RsyncFileAttributes
{
    private final int _mode;
    private final long _size;
    private final long _lastModified;
    private final User _user;
    private final Group _group;
    private final long _inode;
    private final int _nlink;

    /**
     * @throws IllegalArgumentException if fileSize and/or lastModified is
     *         negative
     */
    public RsyncFileAttributes(int mode, long fileSize, long lastModified,
                               User user, Group group, int nlink, long inode)
    {
        assert user != null;
        assert group != null;
        if (fileSize < 0) {
            throw new IllegalArgumentException(String.format(
                "illegal negative file size %d", fileSize));
        }
        if (lastModified < 0) {
            throw new IllegalArgumentException(String.format(
                "illegal negative last modified time %d", lastModified));
        }
        _mode = mode;
        _size = fileSize;
        _lastModified = lastModified;
        _user = user;
        _group = group;
        _inode = inode;
        _nlink = nlink;
    }

    public RsyncFileAttributes(int mode, long fileSize, long lastModified,
                    User user, Group group) {
        this( mode, fileSize, lastModified, user, group, 1, 0l );
    }
    
    @Override
    public String toString()
    {
        return String.format("%s (type=%s, mode=%#o, size=%d, " +
                             "lastModified=%d, user=%s, group=%s, nlinks=%s, inode=%s)",
                             getClass().getSimpleName(),
                             FileOps.fileTypeToString(_mode),
                             _mode, _size, _lastModified, _user, _group,_nlink,_inode);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj != null && getClass() == obj.getClass()) {
            RsyncFileAttributes other = (RsyncFileAttributes) obj;
            return _lastModified == other._lastModified &&
                   _size         == other._size &&
                   _mode         == other._mode &&
                   _user.equals(other._user) &&
                   _group.equals(other._group) &&
                   _nlink == other._nlink && _inode == other._inode;

        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_lastModified, _size, _mode, _user, _group,_nlink,_inode);
    }

    public int mode()
    {
        return _mode;
    }

    public long lastModifiedTime()
    {
        return _lastModified;
    }

    public long size()
    {
        return _size;
    }

    public User user()
    {
        return _user;
    }

    public Group group()
    {
        return _group;
    }

    public boolean isDirectory()
    {
        return FileOps.isDirectory(_mode);
    }

    public boolean isRegularFile()
    {
        return FileOps.isRegularFile(_mode);
    }

    public boolean isSymbolicLink()
    {
        return FileOps.isSymbolicLink(_mode);
    }

    public boolean isOther()
    {
        return FileOps.isOther(_mode);
    }

    public boolean isBlockDevice()
    {
        return FileOps.isBlockDevice(_mode);
    }

    public boolean isCharacterDevice()
    {
        return FileOps.isCharacterDevice(_mode);
    }

    public boolean isFifo()
    {
        return FileOps.isFIFO(_mode);
    }

    public boolean isSocket()
    {
        return FileOps.isSocket(_mode);
    }

    public int fileType()
    {
        return FileOps.fileType(_mode);
    }
    
    public boolean isHardLink() {
        return _nlink > 1 && !isDirectory();
    }
    
    public long inode() {
        return _inode;
    }
}
