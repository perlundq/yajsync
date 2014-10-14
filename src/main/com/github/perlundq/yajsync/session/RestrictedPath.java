/*
 * Safe path resolving with a module root dir
 *
 * Copyright (C) 2014 Per Lundqvist
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
package com.github.perlundq.yajsync.session;

import java.nio.file.Path;
import java.util.Objects;

import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.PathOps;

/**
 * A RestrictedPath is a representation of a module and its root
 * directory path that provides robust semantics for safely resolving
 * any untrusted path coming from a possible external source. It
 * allows resolving of any path that is below the module root
 * directory and will throw a RsyncSecurityException for any other
 * path.
 */
public final class RestrictedPath
{
    private final Path _moduleName;
    private final Path _rootPath;

    /**
     * @param moduleName non absolute name of Module as a path, must contain
     *        only one relative path component.
     * @param rootPath the absolute path to the module top directory.
     */
    public RestrictedPath(Path moduleName, Path rootPath)
    {
        assert !moduleName.isAbsolute();
        assert moduleName.getNameCount() == 1;
        assert rootPath.isAbsolute();
        assert !moduleName.toString().contains(Text.SLASH);
        _moduleName = moduleName;
        _rootPath = rootPath;
    }

    @Override
    public String toString()
    {
        return String.format("%s(name=%s, root=%s)", getClass().getSimpleName(),
                             _moduleName, _rootPath);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other != null && other.getClass() == getClass()) {
            RestrictedPath otherPath = (RestrictedPath) other;
            return _moduleName.equals(otherPath._moduleName) &&
                   _rootPath.equals(otherPath._rootPath);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_moduleName, _rootPath);
    }

    /**
     * resolve other in a secure manner without any call to stat.
     * @throws RsyncSecurityException
     */
    public Path resolve(Path other)
    {
        Path normalized = normalizeEmptyToDotDir(other);                        // "" -> ".", "MODULE/.." -> ".", "MODULE/a/././../b" -> "MODULE/b"
        if (normalized.startsWith(_moduleName)) {
            if (normalized.getNameCount() == 1) {
                return _rootPath;
            }
            Path strippedOfModulePrefix =
                normalized.subpath(1, normalized.getNameCount());
            return _rootPath.resolve(strippedOfModulePrefix);
        } else {
            throw new RsyncSecurityException(String.format(
                "\"%s\" is outside virtual dir for module %s",
                other, _moduleName));
        }
    }

    // NOTE: might return path prefixed with ..
    private static Path normalizeEmptyToDotDir(Path path)
    {
        if (path.equals(PathOps.EMPTY)) { // otherwise path.normalize will throw an exception
            return PathOps.DOT_DIR;
        }
        Path normalized = path.normalize();
        if (normalized.equals(PathOps.EMPTY)) {
            return PathOps.DOT_DIR;
        }
        return normalized;
    }
}
