/*
 * /etc/rsyncd.conf module information
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
package com.github.perlundq.yajsync.ui;

import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import com.github.perlundq.yajsync.session.RsyncSecurityException;
import com.github.perlundq.yajsync.util.PathOps;

public class Module
{
    public static class Builder {
        private String _name = "";
        private String _comment = "";
        private Path _path;
        private boolean _isReadOnly = true;

        /**
         * @throws IllegalArgumentException
         */
        Builder(String name) {
            if (name == null) {
                throw new IllegalArgumentException("supplied module name is null");
            }
            _name = name;
        }

        @Override
        public String toString() {
            return String.format("%s (name=%s comment=%s path=%s " +
                                 "isReadOnly=%s)",
                                 getClass().getSimpleName(), _name, _comment,
                                 _path, _isReadOnly);
        }

        public String name() {
            return _name;
        }

        public Builder setIsReadOnly(boolean value) {
            _isReadOnly = value;
            return this;
        }

        /**
         * @throws IllegalArgumentException
         */
        public Builder setComment(String comment) {
            if (comment == null) {
                throw new IllegalArgumentException("comment is null");
            }
            _comment = comment;
            return this;
        }

        /**
         * @throws IllegalArgumentException
         */
        public Builder setPath(Path path) throws IOException {
            if (path == null) {
                throw new IllegalArgumentException("path is null");
            }
            _path = path.toRealPath(LinkOption.NOFOLLOW_LINKS);
            if (_path.getNameCount() == 0) {
                throw new IllegalArgumentException("Error: module path may not be the root directory");
            }
            return this;
        }

        public boolean hasPath() {
            return _path != null;
        }

        public boolean isGlobal() {
            return _name.equals("");
        }
    }

    public static final String GLOBAL_MODULE_NAME = "";
    private final String _name;
    private final String _comment;
    private final Path _path;
    private final boolean _isReadOnly;

    public Module(Builder builder)
    {
        _name = builder._name;
        _comment = builder._comment;
        _path = builder._path;
        _isReadOnly = builder._isReadOnly;
    }

    @Override
    public String toString()
    {
        // TODO: print only non-null values
        return String.format("[%s]:%n\tcomment = %s\n\tpath = %s\n\tread only = %s%n",
                             _name, _comment, _path, _isReadOnly);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other != null && getClass() == other.getClass()) {
            Module otherModule = (Module) other;
            return name().equals(otherModule.name());
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_name);
    }

    public boolean isGlobal()
    {
        return _name.equals(GLOBAL_MODULE_NAME);
    }

    public boolean isReadOnly()
    {
        return _isReadOnly;
    }

    public String name()
    {
        return _name;
    }

    public String comment()
    {
        return _comment;
    }

    public Path resolveVirtual(Path other)
    {
        Path normalized = normalizeEmptyToDotDir(other);                        // e.g. MODULE/path/to/file
        Path moduleNameAsPath = Paths.get(_name);
        if (!normalized.startsWith(moduleNameAsPath)) {                         // NOTE: any absolute paths will throw here, as will MODULE/..
            throw new RsyncSecurityException(String.format(
                "\"%s\" is outside module virtual top dir %s", other, _name));
        }

        int moduleNameCount = moduleNameAsPath.getNameCount();
        if (normalized.getNameCount() == moduleNameCount) {
            return _path;
        }
        Path strippedOfModulePrefix =
            normalized.subpath(moduleNameCount,
                               normalized.getNameCount());
        return resolve(strippedOfModulePrefix);
    }

    // NOTE: may return SAFE/PATH/TO/MODULE_TOP_DIR/..
    private Path resolve(Path other)
    {
        Path result = resolveOrNull(other);
        if (result == null) {
            throw new RsyncSecurityException(String.format(
                "%s is outside module top dir %s", other, _path));
        }
        return result;
    }

    // NOTE: may return SAFE/PATH/TO/MODULE_TOP_DIR/..
    private Path resolveOrNull(Path other)
    {
        Path normalized = normalizeEmptyToDotDir(other);
        if (!normalized.isAbsolute()) {
            normalized = _path.resolve(normalized);
        }
        if (normalized.startsWith(_path)) {
            return normalized;
        }
        return null;
    }

    // NOTE: might return path prefixed with ..
    private static Path normalizeEmptyToDotDir(Path path)
    {
        Path normalized = path.normalize();
        if (normalized.equals(PathOps.EMPTY)) {
            return PathOps.DOT_DIR;
        }
        return normalized;
    }
}
