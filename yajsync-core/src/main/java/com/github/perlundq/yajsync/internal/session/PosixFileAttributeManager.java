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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.github.perlundq.yajsync.attr.Group;
import com.github.perlundq.yajsync.attr.RsyncFileAttributes;
import com.github.perlundq.yajsync.attr.User;
import com.github.perlundq.yajsync.internal.util.FileOps;

public final class PosixFileAttributeManager extends FileAttributeManager
{
    private final int _defaultUserId;
    private final int _defaultGroupId;

    public PosixFileAttributeManager(int defaultUserId, int defaultGroupId)
    {
        _defaultUserId = defaultUserId;
        _defaultGroupId = defaultGroupId;
    }

    @Override
    public RsyncFileAttributes stat(Path path) throws IOException
    {
        PosixFileAttributes attrs = Files.readAttributes(path, PosixFileAttributes.class,
                                                         LinkOption.NOFOLLOW_LINKS);
        return new RsyncFileAttributes(toMode(attrs),
                                       attrs.size(),
                                       attrs.lastModifiedTime().to(TimeUnit.SECONDS),
                                       new User(attrs.owner().getName(), _defaultUserId),
                                       new Group(attrs.group().getName(), _defaultGroupId));
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
