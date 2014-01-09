/*
 * Path utility routines
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
package com.github.perlundq.yajsync.util;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import com.github.perlundq.yajsync.text.Text;

public final class PathOps
{
    public static final Path EMPTY = Paths.get(Text.EMPTY);
    public static final Path DOT_DIR = Paths.get(Text.DOT);
    public static final Path DOT_DOT_DIR = Paths.get(Text.DOT_DOT);
    public static final int MAX_PATH_NAME_LENGTH = 255;

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

    public static boolean isDirectoryStructurePreservable(String unixPathName)
    {
        assert unixPathName != null;
        if (Environment.IS_PATH_SEPARATOR_SLASH) {
            return true;
        }
        return !unixPathName.contains(Environment.PATH_SEPARATOR);
    }

    public static Path parentPath(Path path, int level)
    {
        assert level >= 0;
        assert level <= path.getNameCount();
        assert path.isAbsolute();
        int numComponents = path.getNameCount();
        return path.getRoot().resolve(path.subpath(0, numComponents - level)); // NOTE: Path.getRoot may return null
    }

    public static Path subtractPath(Path absolute, Path subPath)
    {
        assert absolute.endsWith(subPath);
        return parentPath(absolute, subPath.getNameCount());
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
    
//    public static Path resolve(Path path, Path other)
//    {
//        if (other.isAbsolute()) {
//            throw new IllegalArgumentException(other.toString());
//        }
//
//        LinkedList<Path> paths = new LinkedList<>();
//        for (Path p : other) {
//            paths.add(p);
//        }
//        for (Path p : other) {
//            if (p.equals(DOT_DOT_DIR)) {
//                if (paths.isEmpty()) {
//                    throw new InvalidPathException(path.toString() +
//                                                   Environment.PATH_SEPARATOR +
//                                                   other.toString(),
//                                                   "cannot resolve ..");
//                }
//                paths.removeLast();
//            } else if (!p.equals(DOT_DIR)) {
//                paths.add(p);
//            }
//        }
//        return joinPaths(path, paths);
//    }
    
    public static Path normalizeStrict(Path path)
    {
        if (Environment.IS_RUNNING_WINDOWS) {
            return normalizePathWin(path);
        }
        return normalizePathDefault(path);
    }

    private static Path joinPaths(Path path, List<Path> paths)
    {
        Path result = path.isAbsolute() ? path.getRoot() : EMPTY;
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
        for (Path p : path) {
            if (p.equals(DOT_DOT_DIR)) {
                if (paths.isEmpty()) {
                    throw new InvalidPathException(path.toString(),
                                                   "cannot resolve ..");
                }
                paths.removeLast();
            } else {
                String normalized =
                    Text.deleteTrailingDots(p.toString()).toLowerCase();
                if (!normalized.isEmpty()) {
                    paths.add(Paths.get(normalized));
                }
            }
        }
        return joinPaths(path, paths);
    }
  
    // we can't use Path.normalize because it resolves a/../.. -> .. for example
    private static Path normalizePathDefault(Path path)
    {
        LinkedList<Path> paths = new LinkedList<>();
        for (Path p : path) {
            if (p.equals(DOT_DOT_DIR)) {
                if (paths.isEmpty()) {
                    throw new InvalidPathException(path.toString(),
                                                   "cannot resolve ..");
                }
                paths.removeLast();
            } else if (!p.equals(DOT_DIR)) {
                paths.add(p);
            }
        }
        return joinPaths(path, paths);
    }
}
