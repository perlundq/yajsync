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
package com.github.perlundq.yajsync.attr;

/**
 * A class containing generic file information for any type of file (regular,
 * directory, symbolic link or device file).
 *
 * Warning: not designed to be implemented or extended externally.
 *
 * Use attrs() to determine the actual file type. If possible, a more specific
 * subtype will be used, representing symbolic links (SymlinkInfo) and device
 * files (DeviceInfo) respectively. But in some circumstances a symbolic link or
 * device file might be represented as a regular FileInfo - if it is a symlink
 * and lacks information about symlink target or if it is a device file and
 * lacks information about major and minor number.
 *
 * Also, if the file has a local path representation, an as specific as possible
 * instance of LocatableFileInfo will be used.
 */
public interface FileInfo extends Comparable<FileInfo>
{
    RsyncFileAttributes attrs();
    String pathName();
}
