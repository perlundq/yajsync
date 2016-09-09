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
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

import com.github.perlundq.yajsync.attr.Group;
import com.github.perlundq.yajsync.attr.RsyncFileAttributes;
import com.github.perlundq.yajsync.attr.User;
import com.github.perlundq.yajsync.internal.util.FileOps;

public class BasicFileAttributeManager extends FileAttributeManager
{
    private final User _defaultUser;
    private final Group _defaultGroup;
    private final int _defaultFilePermissions;
    private final int _defaultDirectoryPermissions;

    public BasicFileAttributeManager(User defaultUser, Group defaultGroup,
                                    int defaultFilePermissions, int defaultDirectoryPermissions)
    {
        _defaultUser = defaultUser;
        _defaultGroup = defaultGroup;
        _defaultFilePermissions = defaultFilePermissions;
        _defaultDirectoryPermissions = defaultDirectoryPermissions;
    }

    @Override
    public RsyncFileAttributes stat(Path path) throws IOException
    {
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class,
                                                         LinkOption.NOFOLLOW_LINKS);
        return new RsyncFileAttributes(toMode(attrs),
                                       attrs.size(),
                                       attrs.lastModifiedTime().to(TimeUnit.SECONDS),
                                       _defaultUser, _defaultGroup);
    }

    private int toMode(BasicFileAttributes attrs)
    {
        if (attrs.isDirectory()) {
            return FileOps.S_IFDIR | _defaultDirectoryPermissions;
        } else if (attrs.isRegularFile()) {
            return FileOps.S_IFREG | _defaultFilePermissions;
        } else if (attrs.isSymbolicLink()) {
            return FileOps.S_IFLNK | _defaultFilePermissions;
        } else {
            return FileOps.S_IFUNK;
        }
    }
}
