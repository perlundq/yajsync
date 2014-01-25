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
package com.github.perlundq.yajsync.filelist;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.FileOps;

public class RsyncFileAttributes
{
    private final int _mode;
    private final long _size;
    private final long _lastModified;

    /**
     * @throws IllegalArgumentException if fileSize and/or lastModified is
     *         negative
     */
    public RsyncFileAttributes(int mode, long fileSize, long lastModified)
    {
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
    }

    private RsyncFileAttributes(BasicFileAttributes attrs)
    {
        this(RsyncFileAttributes.toMode(attrs),
             attrs.size(),
             attrs.lastModifiedTime().to(TimeUnit.SECONDS));
    }

    public static RsyncFileAttributes stat(Path path) throws IOException
    {
        // this seem to add significant overhead to the native stat system call
        BasicFileAttributes attrs =
            Files.readAttributes(path, BasicFileAttributes.class);
        return new RsyncFileAttributes(attrs);
    }

    public static RsyncFileAttributes statOrNull(Path path)
    {
        try {
            return stat(path);
        } catch (IOException e) {
            return null;
        }
    }

    public static RsyncFileAttributes statIfExists(Path path) throws IOException
    {
        try {
            return stat(path);
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s (type=%s, mode=%#o, size=%d, lastModified=%d)",
                             getClass().getSimpleName(),
                             FileOps.fileTypeToString(_mode),
                             _mode, _size, _lastModified);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj != null && getClass() == obj.getClass()) {
            RsyncFileAttributes other = (RsyncFileAttributes) obj;
            return _lastModified == other._lastModified &&
                   _size         == other._size &&
                   _mode         == other._mode;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_lastModified, _size, _mode);
    }

    public boolean isSettableAttributesEquals(Object obj)
    {
        if (obj != null && getClass() == obj.getClass()) {
            RsyncFileAttributes other = (RsyncFileAttributes) obj;
            return _lastModified == other._lastModified;
        }
        return false;
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

    public int fileType()
    {
        return FileOps.fileType(_mode);
    }

    /**
     * @throws IllegalStateException if attrs is not describing a file directory
     *         or a symbolic link
     */
    private static int toMode(BasicFileAttributes attrs)
    {
        if (attrs.isDirectory()) {
            return FileOps.S_IFDIR | Environment.DEFAULT_DIR_PERMS;
        } else if (attrs.isRegularFile()) {
            return FileOps.S_IFREG | Environment.DEFAULT_FILE_PERMS;
        } else if (attrs.isSymbolicLink()) { // NOTE: we can't modify permissions on the symlink anyway
            return FileOps.S_IFLNK | Environment.DEFAULT_FILE_PERMS;
        } else {
            throw new IllegalStateException(String.format(
                "%s is neither a dir, regular file or a symlink"));
        }
    }
}
