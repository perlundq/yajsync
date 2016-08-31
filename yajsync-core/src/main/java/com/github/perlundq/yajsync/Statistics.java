/*
 * rsync network protocol statistics
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
package com.github.perlundq.yajsync;

public interface Statistics
{
    /**
     * @return total amount of files in all file list segments
     */
    int numFiles();

    /**
     * @return total amount of files transferred
     */
    int numTransferredFiles();

    /**
     * @return total amount of data received from peer (in bytes)
     */
    long totalBytesRead();

    /**
     * @return total amount of data sent to peer (in bytes)
     */
    long totalBytesWritten();

    /**
     * @return total size of all files in all segments (in bytes)
     */
    long totalFileSize();

    /**
     * @return total size of all files transferred (in bytes)
     */
    long totalTransferredSize();

    /**
     * @return total amount of transferred literal file data (in bytes)
     */
    long totalLiteralSize();

    /**
     * @return total amount of matched file data (in bytes)
     */
    long totalMatchedSize();

    /**
     * @return total file list size (in bytes)
     */
    long totalFileListSize();

    /**
     * @return time for the generation and transfer of the initial segment
     *     excluding sending the end of segment (in milliseconds). Warning:
     *     not really useful since yajsync uses incremental recursion only (i.e.
     *     it splits up the file list into several segments).
     */
    long fileListBuildTime();

    /**
     * @return time for sending of end of initial segment and additional meta
     *     data (in milliseconds). Warning: not really useful since yajsync uses
     *     incremental recursion only (i.e. it splits up the file list into
     *     several segments).
     */
    long fileListTransferTime();
}
