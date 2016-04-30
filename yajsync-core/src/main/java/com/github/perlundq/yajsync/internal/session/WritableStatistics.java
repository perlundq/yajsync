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

public class WritableStatistics implements Statistics
{
    // derived by both sender and receiver separately
    private int _numFiles;
    private int _numTransferredFiles;
    private long _totalFileListSize;
    private long _totalTransferredSize;
    private long _totalLiteralSize;
    private long _totalMatchedSize;

    // derived by sender, received by receiver
    private long _totalFileSize;
    private long _totalBytesRead;
    private long _totalBytesWritten;
    private long _fileListBuildTime;
    private long _fileListTransferTime;

    public void setNumFiles(int numFiles)
    {
        _numFiles = numFiles;
    }

    @Override
    public int numFiles()
    {
        return _numFiles;
    }

    public void setNumTransferredFiles(int numTransferredFiles)
    {
        _numTransferredFiles = numTransferredFiles;
    }

    @Override
    public int numTransferredFiles()
    {
        return _numTransferredFiles;
    }

    public void setTotalRead(long totalRead)
    {
        _totalBytesRead = totalRead;
    }

    @Override
    public long totalBytesRead()
    {
        return _totalBytesRead;
    }

    public void setTotalBytesWritten(long totalBytesWritten)
    {
        _totalBytesWritten = totalBytesWritten;
    }

    @Override
    public long totalBytesWritten()
    {
        return _totalBytesWritten;
    }

    public void setTotalFileSize(long totalFileSize)
    {
        _totalFileSize = totalFileSize;
    }

    @Override
    public long totalFileSize()
    {
        return _totalFileSize;
    }

    public void setTotalFileListSize(long totalFileListSize)
    {
        _totalFileListSize = totalFileListSize;
    }

    @Override
    public long totalFileListSize()
    {
        return _totalFileListSize;
    }

    public void setFileListBuildTime(long fileListBuildTime)
    {
        _fileListBuildTime = fileListBuildTime;
    }

    @Override
    public long fileListBuildTime()
    {
        return _fileListBuildTime;
    }

    public void setFileListTransferTime(long fileListTransferTime)
    {
        _fileListTransferTime = fileListTransferTime;
    }

    @Override
    public long fileListTransferTime()
    {
        return _fileListTransferTime;
    }

    public void setTotalLiteralSize(long totalLiteralSize)
    {
        _totalLiteralSize = totalLiteralSize;
    }

    @Override
    public long totalLiteralSize()
    {
        return _totalLiteralSize;
    }

    public void setTotalMatchedSize(long totalMatchedSize)
    {
        _totalMatchedSize = totalMatchedSize;
    }

    @Override
    public long totalMatchedSize()
    {
        return _totalMatchedSize;
    }

    public void setTotalTransferredSize(long totalTransferredSize)
    {
        _totalTransferredSize = totalTransferredSize;
    }

    @Override
    public long totalTransferredSize()
    {
        return _totalTransferredSize;
    }
}
