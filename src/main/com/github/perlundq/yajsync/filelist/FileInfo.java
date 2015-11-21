/*
 * Rsync file information
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

import java.nio.file.Path;
import java.util.Objects;

import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.PathOps;

/**
 * a class for rsync file information, we must keep the path name as a string
 * as opposed to storing the Path directly since the latter may fail
 * (when being the receiver) and we must keep the file list identical to the
 * peer's file list when being receiver
 */
public class FileInfo implements Comparable<FileInfo>
{
    private final Path _path;                                                   // full path to file
    private final Path _normalizedPath;                                         // normalized relative path to receiver destination directory
    private final byte[] _pathNameBytes;                                        // name of relative path to receiver destination (in bytes)
    private final RsyncFileAttributes _attrs;

    // possibly remove and replace with external per thread bitmaps instead?
    private boolean _isPruned = false;          // used by generator only (and only for directories)
    private boolean _isTransferred = false;     // used by receiver (and sender)

    /**
     * @throws IllegalArgumentException if file has a trailing slash but is not
     *         a directory
     */
    public FileInfo(Path path, Path normalizedPath, byte[] pathNameBytes,
                    RsyncFileAttributes attrs)
    {
        assert pathNameBytes != null && pathNameBytes.length > 0;
        assert attrs != null;

        boolean isTrailingSlash =
            pathNameBytes[pathNameBytes.length - 1] == Text.ASCII_SLASH;
        if (isTrailingSlash && !attrs.isDirectory()) {
            throw new IllegalArgumentException(String.format(
                "%s has a trailing slash but is not a directory (path=%s, " +
                "attrs=%s)", Text.bytesToString(pathNameBytes), path, attrs));
        }
        _path = path;
        _normalizedPath = normalizedPath;
        _pathNameBytes = !isTrailingSlash && attrs.isDirectory() // we could possibly make a defensive copy of it since it's mutable, on the other hand we'd just try not to modify it instead
                             ? addSlash(pathNameBytes)
                             : pathNameBytes;
        _attrs = attrs;
    }

    /**
     *  not consistent with equals when using windows
     *  i.e. compareTo result != 0 and equals true is possible,
     *  but compareTo == 0 implies equals == true
     */
    @Override
    public int compareTo(FileInfo other)
    {
        int result = compareUnixFileNamesBytes(_pathNameBytes,
                                               _attrs.isDirectory(),
                                               other._pathNameBytes,
                                               other._attrs.isDirectory());
        assert result != 0 || this.equals(other);
        return result;
    }

    @Override
    public String toString()
    {
        return String.format("%s (attrs=%s, path=%s)",
                             getClass().getSimpleName(), _attrs, _path);
    }

    // two FileInfo instances are considered equal if the resulting real path is
    // identical
    // not consistent with compareTo if running windows see comment for compareTo
    @Override
    public boolean equals(Object other)
    {
        if (other != null && getClass() == other.getClass()) {
            FileInfo otherFile = (FileInfo) other;
            if (_normalizedPath == null || otherFile._normalizedPath == null) {
                return _normalizedPath == otherFile._normalizedPath;
            }
            return _normalizedPath.equals(otherFile._normalizedPath);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_normalizedPath);
    }

    public RsyncFileAttributes attrs()
    {
        return _attrs;
    }

    public boolean isDotDir()
    {
        return _attrs.isDirectory() && isDotDir(_pathNameBytes);
    }

    // NOTE: may be null for receiver/generator, never null for sender
    public Path path()
    {
        return _path;
    }

    /**
     * WARNING: the result is undefined if the returned array is modified, it
     * should be considered immutable
     * @return
     */
    public byte[] pathNameBytes()
    {
//      return _pathNameBytes.clone();
        return _pathNameBytes;
    }

    public void setIsTransferred()
    {
        assert _attrs.isRegularFile();
        _isTransferred = true;
    }

    public boolean isTransferred()
    {
        return _isTransferred;
    }

    public void prune()
    {
        assert _attrs.isDirectory();
        _isPruned = true;
    }

    public boolean isPruned()
    {
        return _isPruned;
    }

    public boolean isTransferrable()
    {
        return _path != null;
    }


    private static byte[] addSlash(byte[] pathNameBytes)
    {
        byte[] result = new byte[pathNameBytes.length + 1];
        System.arraycopy(pathNameBytes, 0, result, 0, pathNameBytes.length);
        result[result.length - 1] = Text.ASCII_SLASH;
        return result;
    }

    private static boolean isDirComponent(byte[] bytes, int offset)
    {
        for (int i = offset; i < bytes.length; i++) {
            if (bytes[i] == Text.ASCII_SLASH) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDotDir(byte[] bytes)
    {
        return bytes.length == 2 &&
               bytes[0] == Text.ASCII_DOT &&
               bytes[1] == Text.ASCII_SLASH;          // REDUNDANT if we know it's a directory, we always add slashes to dirs
    }

    private static int cmpDotDirFirstElseDirAfterFiles(int diff,
                                                       byte[] leftBytes,
                                                       boolean isLeftDir,
                                                       byte[] rightBytes,
                                                       boolean isRightDir,
                                                       int offset)
    {
        if (isDotDir(leftBytes)) {
            assert isLeftDir;
            if (isDotDir(rightBytes)) {
                assert isRightDir && diff == 0;
                return 0;
            }
            return -1;
        } else if (isDotDir(rightBytes)) {
            assert isRightDir;
            return 1;
        }

        boolean isLeftADirComponent = isLeftDir ||
                                      isDirComponent(leftBytes, offset);
        boolean isRightADirComponent = isRightDir ||
                                       isDirComponent(rightBytes, offset);
        if (isLeftADirComponent == isRightADirComponent) {
            return diff;
        } else if (isLeftADirComponent) {
            assert !isRightADirComponent;
            return 1;
        } else {
            assert isRightADirComponent && !isLeftADirComponent;
            return -1;
        }
    }

    private static int cmpSubPaths(boolean isLeftAtEnd, boolean isRightAtEnd)
    {
        if (isLeftAtEnd == isRightAtEnd) {
            return 0;
        } else if (isLeftAtEnd) {
            return -1;
        } // else if (isRightAtEnd) {
        return 1;
    }

    /*
     * sort . (always a dir) before anything else
     * sort files before dirs on the same level
     * compare a dir using a trailing slash
     *
     * NOTE: this won't sort dot dirs within a path first for that level
     */
    private static int compareUnixFileNamesBytes(byte[] leftBytes,
                                                 boolean isLeftDir,
                                                 byte[] rightBytes,
                                                 boolean isRightDir)
    {
        int i = 0;
        for (; i < leftBytes.length && i < rightBytes.length; i++) {
            int diff = (0xFF & leftBytes[i]) - (0xFF & rightBytes[i]);
            if (diff != 0) {
                return cmpDotDirFirstElseDirAfterFiles(diff,
                                                       leftBytes, isLeftDir,
                                                       rightBytes, isRightDir,
                                                       i);
            }
        }
        // one or both are at the end and
        // one or both are a substring of the other
        boolean isLeftAtEnd = i == leftBytes.length;
        boolean isRightAtEnd = i == rightBytes.length;
        int diff = cmpSubPaths(isLeftAtEnd, isRightAtEnd);
        return cmpDotDirFirstElseDirAfterFiles(diff, leftBytes, isLeftDir,
                                               rightBytes, isRightDir, i);
    }
}
