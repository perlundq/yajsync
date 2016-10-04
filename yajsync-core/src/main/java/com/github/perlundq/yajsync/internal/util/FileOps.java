/*
 * File utility routines
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
package com.github.perlundq.yajsync.internal.util;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import com.github.perlundq.yajsync.attr.RsyncFileAttributes;
import com.github.perlundq.yajsync.internal.text.Text;

public class FileOps
{
    public static final int S_IFMT   = 0170000; // bit mask for the file type bit fields
    public static final int S_IFSOCK = 0140000; // socket
    public static final int S_IFLNK  = 0120000; // symbolic link
    public static final int S_IFREG  = 0100000; // regular file
    public static final int S_IFBLK  = 0060000; // block device
    public static final int S_IFDIR  = 0040000; // directory
    public static final int S_IFCHR  = 0020000; // character device
    public static final int S_IFIFO  = 0010000; // FIFO

    // NOTE: unconventional - added to be able to represent files of type other
    // (e.g. from PosixFileAttributes)
    public static final int S_IFUNK  = 0150000;

    public static final int S_ISUID  = 0004000; // set UID bit
    public static final int S_ISGID  = 0002000; // set-group-ID bit
    public static final int S_ISVTX  = 0001000; // sticky bit

    public static final int S_IRWXU  = 0000700; // mask for file owner permissions
    public static final int S_IRUSR  = 0000400; // owner has read permission
    public static final int S_IWUSR  = 0000200; // owner has write permission
    public static final int S_IXUSR  = 0000100; // owner has execute permission

    public static final int S_IRWXG  = 0000070; // mask for group permissions
    public static final int S_IRGRP  = 0000040; // group has read permission
    public static final int S_IWGRP  = 0000020; // group has write permission
    public static final int S_IXGRP  = 0000010; // group has execute permission

    public static final int S_IRWXO  = 0000007; // mask for permissions for others
    public static final int S_IROTH  = 0000004; // others have read permission
    public static final int S_IWOTH  = 0000002; // others have write permission
    public static final int S_IXOTH  = 0000001; // others have execute permission

    private static final int SHIFT_IRWXU = 6;
    private static final int SHIFT_IRWXG = 3;

    private static final String UNKNOWN_STR = "unknown";
    private static final String SOCK_STR    = "socket";
    private static final String LINK_STR    = "link";
    private static final String REG_STR     = "file";
    private static final String BLOCK_STR   = "block device";
    private static final String DIR_STR     = "directory";
    private static final String CHAR_STR    = "character device";
    private static final String FIFO_STR    = "fifo";

    private static final String S_UNKNOWN_STR = "?";
    private static final String S_SOCK_STR    = "s";
    private static final String S_LINK_STR    = "l";
    private static final String S_REG_STR     = "-";
    private static final String S_BLOCK_STR   = "b";
    private static final String S_DIR_STR     = "d";
    private static final String S_CHAR_STR    = "c";
    private static final String S_FIFO_STR    = "p";


    public static String fileTypeToString(int mode)
    {
        switch (fileType(mode)) {
        case S_IFSOCK:
            return SOCK_STR;
        case S_IFLNK:
            return LINK_STR;
        case S_IFREG:
            return REG_STR;
        case S_IFBLK:
            return BLOCK_STR;
        case S_IFDIR:
            return DIR_STR;
        case S_IFCHR:
            return CHAR_STR;
        case S_IFIFO:
            return FIFO_STR;
        default:
            return UNKNOWN_STR;
        }
    }

    private static String shortfileTypeToString(int mode)
    {
        switch (fileType(mode)) {
        case S_IFSOCK:
            return S_SOCK_STR;
        case S_IFLNK:
            return S_LINK_STR;
        case S_IFREG:
            return S_REG_STR;
        case S_IFBLK:
            return S_BLOCK_STR;
        case S_IFDIR:
            return S_DIR_STR;
        case S_IFCHR:
            return S_CHAR_STR;
        case S_IFIFO:
            return S_FIFO_STR;
        default:
            return S_UNKNOWN_STR;
        }
    }

    public static boolean isUserReadable(int mode)
    {
        return (mode & S_IRUSR) != 0;
    }

    public static boolean isUserWritable(int mode)
    {
        return (mode & S_IWUSR) != 0;
    }

    public static boolean isUserExecutable(int mode)
    {
        return (mode & S_IXUSR) != 0;
    }

    public static boolean isGroupReadable(int mode)
    {
        return (mode & S_IRGRP) != 0;
    }

    public static boolean isGroupWritable(int mode)
    {
        return (mode & S_IWGRP) != 0;
    }

    public static boolean isGroupExecutable(int mode)
    {
        return (mode & S_IXGRP) != 0;
    }

    public static boolean isOtherReadable(int mode)
    {
        return (mode & S_IROTH) != 0;
    }

    public static boolean isOtherWritable(int mode)
    {
        return (mode & S_IWOTH) != 0;
    }

    public static boolean isOtherExecutable(int mode)
    {
        return (mode & S_IXOTH) != 0;
    }

    private static String permOtherToString(int mode)
    {
        StringBuilder sb = new StringBuilder();
        if (isOtherReadable(mode)) {
            sb.append("r");
        } else {
            sb.append("-");
        }
        if (isOtherWritable(mode)) {
            sb.append("w");
        } else {
            sb.append("-");
        }
        if (isOtherExecutable(mode)) {
            sb.append("x");
        } else {
            sb.append("-");
        }
        return sb.toString();
    }

    private static String permGroupToString(int mode)
    {
        return permOtherToString(mode >>> SHIFT_IRWXG);
    }

    private static String permUserToString(int mode)
    {
        return permOtherToString(mode >>> SHIFT_IRWXU);
    }

    // TODO: add support for set uid, set gid and sticky bit
    public static String modeToString(int mode)
    {
        return shortfileTypeToString(mode) +
               permUserToString(mode) +
               permGroupToString(mode) +
               permOtherToString(mode);
    }

    public static int fileType(int mode)
    {
        return mode & S_IFMT;
    }

    public static boolean isRegularFile(int mode)
    {
        return fileType(mode) == S_IFREG;
    }

    public static boolean isDirectory(int mode)
    {
        return fileType(mode) == S_IFDIR;
    }

    public static boolean isSymbolicLink(int mode)
    {
        return fileType(mode) == S_IFLNK;
    }

    public static boolean isOther(int mode)
    {
        return !isRegularFile(mode) &&
               !isSymbolicLink(mode) &&
               !isDirectory(mode);
    }

    public static boolean isSocket(int mode)
    {
        return fileType(mode) == S_IFSOCK;
    }

    public static boolean isBlockDevice(int mode)
    {
        return fileType(mode) == S_IFBLK;
    }

    public static boolean isCharacterDevice(int mode)
    {
        return fileType(mode) == S_IFCHR;
    }

    public static boolean isFIFO(int mode)
    {
        return fileType(mode) == S_IFIFO;
    }

    // this can fail in many ways, maybe we can try to recover some of them?
    public static boolean atomicMove(Path tempFile, Path path)
    {
        try {
            Files.move(tempFile, path, StandardCopyOption.ATOMIC_MOVE);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static long sizeOf(Path file)
    {
        try {
            return Files.size(file);
        } catch (IOException e) {
            return -1;
        }
    }

    public static void unlink(Path path) throws IOException
    {
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            try (DirectoryStream<Path> stream =
                    Files.newDirectoryStream(path)) {
                for (Path entry : stream) {
                    if (!entry.equals(path)) {
                        assert (entry.normalize().startsWith(path.normalize()));
                        unlink(entry);
                    }
                }
            }
        }
        Files.deleteIfExists(path);
    }

    // rsync explicitly disallows empty symlinks
    public static Path readLinkTarget(Path path) throws IOException
    {
        Path symlinkTarget = Files.readSymbolicLink(path);
        Path empty = path.getFileSystem().getPath(Text.EMPTY);
        if (symlinkTarget.equals(empty)) {
            throw new IOException("symlink target is the empty string");
        }
        return symlinkTarget;
    }

    public static boolean isDataModified(RsyncFileAttributes curAttrsOrNull,
                                         RsyncFileAttributes newAttrs)
    {
        assert newAttrs != null;
        return curAttrsOrNull == null ||
               curAttrsOrNull.size() != newAttrs.size() ||
               curAttrsOrNull.lastModifiedTime() != newAttrs.lastModifiedTime();
    }
}
