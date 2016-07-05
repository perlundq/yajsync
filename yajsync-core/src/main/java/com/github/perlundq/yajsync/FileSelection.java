/*
 * Copyright (C) 2013-2016 Per Lundqvist
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
package com.github.perlundq.yajsync;

/**
 * An rsync transfer handles the initial list of file arguments sent by the
 * client in three different ways: 1. transfer the initial files exactly but
 * exclude all directories 2. transfer the initial files but also recurse into
 * directories 3. transfer the initial files but also transfer the contents
 * of any dot directories (i.e. the name ends with a trailing slash "dir/"' or a
 * slash followed by one dot "dir/."
 */
public enum FileSelection
{
    /**
     * Transfer the initial client file list literally while excluding
     * directories.
     */
    EXACT,

    /**
     * Transfer the initial client file list but also recurse into directories.
     */
    RECURSE,

    /**
     * Transfer the initial client file list and the contents of any dot
     * directories.
     */
    TRANSFER_DIRS
}
