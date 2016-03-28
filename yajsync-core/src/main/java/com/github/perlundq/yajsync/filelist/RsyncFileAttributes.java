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
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.FileOps;

public class RsyncFileAttributes
{
    private final int _mode;
    private final long _size;
    private final long _lastModified;
    private final User _user;
    private final Group _group;

    /**
     * @throws IllegalArgumentException if fileSize and/or lastModified is
     *         negative
     */
    public RsyncFileAttributes(int mode, long fileSize, long lastModified,
                               User user, Group group)
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
    }

    private RsyncFileAttributes(BasicFileAttributes attrs)
    {
        this(RsyncFileAttributes.toMode(attrs),
             attrs.size(),
             attrs.lastModifiedTime().to(TimeUnit.SECONDS),
             User.JVM_USER,
             Group.JVM_GROUP);
    }

    private RsyncFileAttributes(PosixFileAttributes attrs)
    {
        this(RsyncFileAttributes.toMode(attrs),
             attrs.size(),
             attrs.lastModifiedTime().to(TimeUnit.SECONDS),
             new User(attrs.owner().getName(), User.JVM_USER.id()),
             new Group(attrs.group().getName(), Group.JVM_GROUP.id()));
    }

    private static boolean isUnixFileSystem(Path path)
    {
        return path.
                getFileSystem().
                supportedFileAttributeViews().
                contains("unix");
    }

    private static boolean isPosixFileSystem(Path path)
    {
        return path.
                getFileSystem().
                supportedFileAttributeViews().
                contains("posix");
    }

    public static RsyncFileAttributes stat(Path path) throws IOException
    {
        if (isUnixFileSystem(path)) {
            Map<String, Object> attrs =
                Files.readAttributes(path, "unix:lastModifiedTime,mode,size,uid,owner,gid,group",
                                     LinkOption.NOFOLLOW_LINKS);
            long mtime = ((FileTime) attrs.get("lastModifiedTime")).to(TimeUnit.SECONDS);
            int mode = (int) attrs.get("mode");
            long size = (long) attrs.get("size");
            String user = ((UserPrincipal) attrs.get("owner")).getName();
            int uid = (int) attrs.get("uid");
            String group = ((GroupPrincipal) attrs.get("group")).getName();
            int gid = (int) attrs.get("gid");
            return new RsyncFileAttributes(mode, size, mtime,
                                           new User(user, uid), new Group(group, gid));
        } else if (isPosixFileSystem(path)) {
            PosixFileAttributes attrs =
                Files.readAttributes(path, PosixFileAttributes.class,
                                     LinkOption.NOFOLLOW_LINKS);
            return new RsyncFileAttributes(attrs);
        } else {
            BasicFileAttributes attrs =
                Files.readAttributes(path, BasicFileAttributes.class,
                                     LinkOption.NOFOLLOW_LINKS);
            return new RsyncFileAttributes(attrs);
        }
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
        return String.format("%s (type=%s, mode=%#o, size=%d, " +
                             "lastModified=%d, user=%s, group=%s)",
                             getClass().getSimpleName(),
                             FileOps.fileTypeToString(_mode),
                             _mode, _size, _lastModified, _user, _group);
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
                   _group.equals(other._group);

        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_lastModified, _size, _mode, _user, _group);
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

    private static int toMode(BasicFileAttributes attrs)
    {
        if (attrs.isDirectory()) {
            return FileOps.S_IFDIR | Environment.DEFAULT_DIR_PERMS;
        } else if (attrs.isRegularFile()) {
            return FileOps.S_IFREG | Environment.DEFAULT_FILE_PERMS;
        } else if (attrs.isSymbolicLink()) { // NOTE: we can't modify permissions on the symlink anyway
            return FileOps.S_IFLNK | Environment.DEFAULT_FILE_PERMS;
        } else {
            return FileOps.S_IFUNK;
        }
    }

    private static int toMode(PosixFileAttributes attrs)
    {
        int mode = 0;

        if (attrs.isDirectory()) {
            mode |= FileOps.S_IFDIR;
        } else if (attrs.isRegularFile()) {
            mode |= FileOps.S_IFREG;
        } else if (attrs.isSymbolicLink()) {
            mode |= FileOps.S_IFLNK;
        } else {
            mode |= FileOps.S_IFUNK;
        }

        Set<PosixFilePermission> perms = attrs.permissions();

        if (perms.contains(PosixFilePermission.OWNER_READ)) {
            mode |= FileOps.S_IRUSR;
        }
        if (perms.contains(PosixFilePermission.OWNER_WRITE)) {
            mode |= FileOps.S_IWUSR;
        }
        if (perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
            mode |= FileOps.S_IXUSR;
        }
        if (perms.contains(PosixFilePermission.GROUP_READ)) {
            mode |= FileOps.S_IRGRP;
        }
        if (perms.contains(PosixFilePermission.GROUP_WRITE)) {
            mode |= FileOps.S_IWGRP;
        }
        if (perms.contains(PosixFilePermission.GROUP_EXECUTE)) {
            mode |= FileOps.S_IXGRP;
        }
        if (perms.contains(PosixFilePermission.OTHERS_READ)) {
            mode |= FileOps.S_IROTH;
        }
        if (perms.contains(PosixFilePermission.OTHERS_WRITE)) {
            mode |= FileOps.S_IWOTH;
        }
        if (perms.contains(PosixFilePermission.OTHERS_EXECUTE)) {
            mode |= FileOps.S_IXOTH;
        }

        return mode;
    }
}
