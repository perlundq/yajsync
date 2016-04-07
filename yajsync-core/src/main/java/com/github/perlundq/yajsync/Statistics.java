/*
 * Statistics required by the rsync network protocol
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
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
package com.github.perlundq.yajsync;

public class Statistics
{
    // derived by both sender and receiver separately
    private int _numFiles;                  // int stats.num_files;                (#) total amount of files for all segments
    private int _numTransferredFiles;       // int stats.num_transferred_files;    (#) total amount of files transferred
    private long _totalFileListSize;        // int64 stats.flist_size;             (bytes) total size of file list 
    private long _totalTransferredSize;     // int64 stats.total_transferred_size; (bytes) total size of all files transferred (regardless of literal/matched data)
    private long _totalLiteralSize;         // int64 stats.literal_data;           (bytes) total amount of transferred literal file data 
    private long _totalMatchedSize;         // int64 stats.matched_data;           (bytes) total amount of matched file data 
    
    // derived by sender, received by receiver
    private long _totalFileSize;            // int64 stats.total_size;             (bytes) total size of all files for all segments (regardless whether transferred or not)
    private long _totalRead;                // int64 stats.total_read;             (bytes) total amount of data received from peer
    private long _totalWritten;             // int64 stats.total_written;          (bytes) total amount of data sent to peer
    private long _fileListBuildTime;        // int64 stats.flist_buildtime;        (ms) generation and transfer of initial segment excluding sending end of segment
    private long _fileListTransferTime;     // int64 stats.flist_xfertime;         (ms) time for sending of end of initial segment + additional meta data   
    
    public void setNumFiles(int numFiles)
    {
        _numFiles = numFiles;
    }

    public int numFiles()
    {
        return _numFiles;
    }

    public void setNumTransferredFiles(int numTransferredFiles)
    {
        _numTransferredFiles = numTransferredFiles;
    }

    public int numTransferredFiles()
    {
        return _numTransferredFiles;
    }

    public void setTotalRead(long totalRead)
    {
        _totalRead = totalRead;
    }

    public long totalRead()
    {
        return _totalRead;
    }

    public void setTotalWritten(long totalWritten)
    {
        _totalWritten = totalWritten;
    }

    public long totalWritten()
    {
        return _totalWritten;
    }

    public void setTotalFileSize(long totalFileSize)
    {
        _totalFileSize = totalFileSize;
    }

    public long totalFileSize()
    {
        return _totalFileSize;
    }

    public void setTotalFileListSize(long totalFileListSize)
    {
        _totalFileListSize = totalFileListSize;
    }

    public long totalFileListSize()
    {
        return _totalFileListSize;
    }

    public void setFileListBuildTime(long fileListBuildTime)
    {
        _fileListBuildTime = fileListBuildTime;
    }
    
    public long fileListBuildTime()
    {
        return _fileListBuildTime;
    }

    public void setFileListTransferTime(long fileListTransferTime)
    {
        _fileListTransferTime = fileListTransferTime;
    }

    public long fileListTransferTime()
    {
        return _fileListTransferTime;
    }

    public void setTotalLiteralSize(long totalLiteralSize)
    {
        _totalLiteralSize = totalLiteralSize;
    }

    public long totalLiteralSize()
    {
        return _totalLiteralSize;
    }

    public void setTotalMatchedSize(long totalMatchedSize)
    {
        _totalMatchedSize = totalMatchedSize;
    }

    public long totalMatchedSize()
    {
        return _totalMatchedSize;
    }    

    public void setTotalTransferredSize(long totalTransferredSize)
    {
        _totalTransferredSize = totalTransferredSize;
    }

    public long totalTransferredSize()
    {
        return _totalTransferredSize;
    }
}
