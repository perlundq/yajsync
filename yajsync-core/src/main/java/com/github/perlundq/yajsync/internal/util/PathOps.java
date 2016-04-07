/*
 * Path utility routines
 *
 * Copyright (C) 2013, 2014, 2016 Per Lundqvist
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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.github.perlundq.yajsync.internal.text.Text;

public final class PathOps
{
    private PathOps() {}

    public static boolean isPathPreservable(Path path)
    {
        assert path != null;
        if (Environment.IS_RUNNING_WINDOWS) {
            return isWindowsPathPreserved(path);
        }
        return true;
    }

    private static boolean isWindowsPathPreserved(Path path)
    {
        assert path != null;
        for (Path p : path) {
            String name = p.toString();
            if (name.endsWith(Text.DOT) &&
                (!name.equals(Text.DOT) && !name.equals(Text.DOT_DOT))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isDirectoryStructurePreservable(String separator,
                                                          String unixPathName)
    {
        assert unixPathName != null;
        if (separator.equals(Text.SLASH)) {
            return true;
        }
        return !unixPathName.contains(separator);
    }

    // / - / = /
    // a - a = null
    // /a - /a = /
    // /a/b/c/d - c/d  = /a/b
    //  a/b/c/d - c/d  = a/b
    // ././. - . = ./.
    public static Path subtractPathOrNull(Path parent, Path sub)
    {
        if (!parent.endsWith(sub)) {
            throw new IllegalArgumentException(String.format(
                    "%s is not a parent path of %s", parent, sub));
        }
        if (parent.getNameCount() == sub.getNameCount()) {
            return parent.getRoot(); // NOTE: return null if parent has no root
        }
        Path res = parent.subpath(0,
                                  parent.getNameCount() - sub.getNameCount());
        if (parent.isAbsolute()) {
            return parent.getRoot().resolve(res);
        } else {
            return res;
        }
    }

    public static boolean contains(Path path, Path searchPath)
    {
        for (Path subPath : path) {
            if (subPath.equals(searchPath)) {
                return true;
            }
        }
        return false;
    }

    public static Path normalizeStrict(Path path)
    {
        if (Environment.IS_RUNNING_WINDOWS) {
            return normalizePathWin(path);
        }
        return normalizePathDefault(path);
    }

    private static Path joinPaths(Path path, List<Path> paths)
    {
        Path empty = path.getFileSystem().getPath(Text.EMPTY);
        Path result = path.isAbsolute() ? path.getRoot() : empty;
        for (Path p : paths) {
            result = result.resolve(p);
        }
        return result;
    }

    /**
     * @throws InvalidPathException if trying to resolve a relative path
     *         prefixed with a .. directory
     */
    private static Path normalizePathWin(Path path)
    {
        LinkedList<Path> paths = new LinkedList<>();
        Path dotDotDir = path.getFileSystem().getPath(Text.DOT_DOT);
        for (Path p : path) {
            if (p.equals(dotDotDir)) {
                if (paths.isEmpty()) {
                    throw new InvalidPathException(path.toString(),
                                                   "cannot resolve ..");
                }
                paths.removeLast();
            } else {
                String normalized =
                    Text.deleteTrailingDots(p.toString()).toLowerCase();
                if (!normalized.isEmpty()) {
                    paths.add(path.getFileSystem().getPath(normalized));
                }
            }
        }
        return joinPaths(path, paths);
    }

    // we can't use Path.normalize because it resolves a/../.. -> .. for example
    private static Path normalizePathDefault(Path path)
    {
        LinkedList<Path> paths = new LinkedList<>();
        Path dotDir = path.getFileSystem().getPath(Text.DOT);
        Path dotDotDir = path.getFileSystem().getPath(Text.DOT_DOT);
        for (Path p : path) {
            if (p.equals(dotDotDir)) {
                if (paths.isEmpty()) {
                    throw new InvalidPathException(path.toString(),
                                                   "cannot resolve ..");
                }
                paths.removeLast();
            } else if (!p.equals(dotDir)) {
                paths.add(p);
            }
        }
        return joinPaths(path, paths);
    }

    /**
     * Preserves trailing slash information (FileSystem.getPath won't) and
     * normalize empty paths to "."
     *
     * @throws InvalidPathException
     */
    public static Path get(FileSystem fs, String name)
    {
        Path normalized = fs.getPath(name).normalize();
        if (normalized.toString().equals(Text.EMPTY)) {
            return fs.getPath(Text.DOT);
        } else if (name.endsWith(Text.SLASH) ||
                   name.endsWith(Text.SLASH + Text.DOT)) {
            return normalized.resolve(Text.DOT);
        } else {
            return normalized;
        }
    }

    public static FileSystem fileSystemOf(String fsPathName)
            throws IOException, URISyntaxException
    {
        URI uri = new URI(fsPathName);
        try {
            return FileSystems.getFileSystem(uri);
        } catch (FileSystemNotFoundException e) {
            Map<String, Object> empty = Collections.emptyMap();
            return FileSystems.newFileSystem(uri, empty);
        }
    }
}
