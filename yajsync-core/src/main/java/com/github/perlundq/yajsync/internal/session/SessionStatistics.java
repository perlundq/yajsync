/*
 * rsync network protocol statistics
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
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
package com.github.perlundq.yajsync.internal.session;

import com.github.perlundq.yajsync.Statistics;

public class SessionStatistics implements Statistics
{
    // NOTE: package private fields
    int _numFiles;
    int _numTransferredFiles;
    long _totalFileListSize;
    long _totalTransferredSize;
    long _totalLiteralSize;
    long _totalMatchedSize;
    long _totalFileSize;
    long _totalBytesRead;
    long _totalBytesWritten;
    long _fileListBuildTime;
    long _fileListTransferTime;

    @Override
    public int numFiles()
    {
        return _numFiles;
    }

    @Override
    public int numTransferredFiles()
    {
        return _numTransferredFiles;
    }

    @Override
    public long totalBytesRead()
    {
        return _totalBytesRead;
    }

    @Override
    public long totalBytesWritten()
    {
        return _totalBytesWritten;
    }

    @Override
    public long totalFileSize()
    {
        return _totalFileSize;
    }

    @Override
    public long totalTransferredSize()
    {
        return _totalTransferredSize;
    }

    @Override
    public long totalLiteralSize()
    {
        return _totalLiteralSize;
    }

    @Override
    public long totalMatchedSize()
    {
        return _totalMatchedSize;
    }

    @Override
    public long totalFileListSize()
    {
        return _totalFileListSize;
    }

    @Override
    public long fileListBuildTime()
    {
        return _fileListBuildTime;
    }

    @Override
    public long fileListTransferTime()
    {
        return _fileListTransferTime;
    }
}
