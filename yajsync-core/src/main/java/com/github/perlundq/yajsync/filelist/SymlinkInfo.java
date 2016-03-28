/*
 * symlink file information
 *
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
package com.github.perlundq.yajsync.filelist;

import java.nio.file.Path;

public class SymlinkInfo extends FileInfo
{
    private final Path _targetPath;

    public SymlinkInfo(Path pathOrNull, Path normalizedPathOrNull,
                       byte[] pathNameBytes, RsyncFileAttributes attrs,
                       Path targetPath)
    {
        super(pathOrNull, normalizedPathOrNull, pathNameBytes, attrs);
        assert targetPath != null;
        assert attrs.isSymbolicLink() : attrs;
        _targetPath = targetPath;
    }

    public Path targetPath()
    {
        return _targetPath;
    }
}
