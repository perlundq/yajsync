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
import java.nio.file.FileSystem;

import com.github.perlundq.yajsync.attr.Group;
import com.github.perlundq.yajsync.attr.User;

public final class FileAttributeManagerFactory
{
    private FileAttributeManagerFactory() {}

    public static FileAttributeManager getMostPerformant(FileSystem fs,
                                                         boolean isPreserveUser,
                                                         boolean isPreserveGroup,
                                                         boolean isPreserveDevices,
                                                         boolean isPreserveSpecials,
                                                         boolean isNumericIds,
                                                         User defaultUser, Group defaultGroup,
                                                         int defaultFilePermissions,
                                                         int defaultDirectoryPermissions)
    {
        if (isUnixSupported(fs) &&
            (isPreserveDevices || isPreserveSpecials || isPreserveUser || isPreserveGroup)) {
            try {
                return new UnixFileAttributeManager(defaultUser, defaultGroup, isPreserveUser,
                                                    isPreserveGroup);
            } catch (IOException e) {
                return new PosixFileAttributeManager(defaultUser.id(), defaultGroup.id());
            }
        } else if (isPosixSupported(fs) && (isPreserveUser || isPreserveGroup)) {
            return new PosixFileAttributeManager(defaultUser.id(), defaultGroup.id());
        } else {
            return new BasicFileAttributeManager(defaultUser, defaultGroup,
                                                 defaultFilePermissions,
                                                 defaultDirectoryPermissions);
        }
    }

    private static boolean isUnixSupported(FileSystem fs)
    {
        return fs.supportedFileAttributeViews().contains("unix");
    }

    private static boolean isPosixSupported(FileSystem fs)
    {
        return fs.supportedFileAttributeViews().contains("posix");
    }

    public static FileAttributeManager newMostAble(FileSystem fs, User defaultUser,
                                                   Group defaultGroup,
                                                   int defaultFilePermissions,
                                                   int defaultDirectoryPermissions)
    {
        if (isUnixSupported(fs)) {
            try {
                return new UnixFileAttributeManager(defaultUser, defaultGroup, true, true);
            } catch (IOException e) {
                // OK: skip to next
            }
        }
        if (isPosixSupported(fs)) {
            return new PosixFileAttributeManager(defaultUser.id(), defaultGroup.id());
        } else {
            return new BasicFileAttributeManager(defaultUser, defaultGroup, defaultFilePermissions,
                                                 defaultDirectoryPermissions);
        }
    }
}
