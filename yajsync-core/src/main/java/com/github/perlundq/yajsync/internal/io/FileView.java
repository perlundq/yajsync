/*
 * Slidable file buffer which defers I/O errors until it is closed and
 * provides direct access to buffer contents
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
package com.github.perlundq.yajsync.internal.io;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.internal.util.RuntimeInterruptException;

public class FileView implements AutoCloseable
{
    private static final Logger _log =
        Logger.getLogger(FileView.class.getName());
    public final static int DEFAULT_BLOCK_SIZE = 8 * 1024;
    private final InputStream _is;
    private final int _windowLength;  // size of sliding window (<= _buf.length)
    private final byte[] _buf;
    private final String _fileName;
    private int _startOffset = 0;
    private int _endOffset = -1;     // length == _endOffset - _startOffset + 1
    private int _markOffset = -1;
    private int _readOffset = -1;
    private long _remainingBytes;
    private IOException _ioError = null;

    public FileView(Path path, long fileSize, int windowLength, int bufferSize)
        throws FileViewOpenFailed
    {
        assert path != null;
        assert fileSize >= 0;
        assert windowLength >= 0;
        assert bufferSize >= 0;
        assert windowLength <= bufferSize;

        try {
            _fileName = path.toString();
            _remainingBytes = fileSize;

            if (fileSize > 0) {
                _is = Files.newInputStream(path);
                _windowLength = windowLength;
                _buf = new byte[bufferSize];
                slide(0);
                assert _startOffset == 0;
                assert _endOffset >= 0;
            } else {
                _is = null;
                _windowLength = 0;
                _buf = new byte[0];
            }

        } catch (FileNotFoundException | NoSuchFileException e) { // TODO: which exception should we really catch
            throw new FileViewNotFound(e.getMessage());
        } catch (IOException e) {
            throw new FileViewOpenFailed(e.getMessage());
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s (fileName=%s, startOffset=%d, markOffset=%d," +
                             " endOffset=%d, windowLength=%d, " +
                             "prefetchedOffset=%d, remainingBytes=%d)",
                             this.getClass().getSimpleName(),
                             _fileName, _startOffset, _markOffset, endOffset(),
                             windowLength(), _readOffset, _remainingBytes);
    }

    @Override
    public void close() throws FileViewException
    {
        if (_is != null) {
            try {
                _is.close();
            } catch (ClosedByInterruptException e) {
                throw new RuntimeInterruptException(e);
            } catch (IOException e) {
                throw new FileViewException(e);
            }
        }

        if (_ioError != null) {
            throw new FileViewException(_ioError);
        }
    }

    public byte[] array()
    {
        return _buf;
    }

    // TODO: the names startOffset and firstOffset are confusingly similar
    public int startOffset()
    {
        assert _startOffset >= 0;
        assert _startOffset <= _buf.length - 1 || _is == null;
        return _startOffset;
    }

    // might return -1
    public int markOffset()
    {
        assert _markOffset >= -1;
        assert _markOffset <= _buf.length - 1 || _is == null;
        return _markOffset;
    }

    // TODO: the names startOffset and firstOffset are confusingly similar
    public int firstOffset()
    {
        assert _startOffset >= 0;
        assert _markOffset >= -1;
        return _markOffset >= 0
                    ? Math.min(_startOffset, _markOffset)
                    : _startOffset;
    }

    public int endOffset()
    {
        assert _endOffset >= -1;
        return _endOffset;
    }

    public void setMarkRelativeToStart(int relativeOffset)
    {
        assert relativeOffset >= 0;
        // it's allowed to move 1 passed _endOffset, as length is defined as:
        // _endOffset - _startOffset + 1
        assert _startOffset + relativeOffset <= _endOffset + 1;
        _markOffset = _startOffset + relativeOffset;
    }

    public int windowLength()
    {
        int length = _endOffset - _startOffset + 1;
        assert length >= 0;
        assert length <= _windowLength : length + " maxWindowLength=" +
                                         _windowLength;
        return length;
    }

    public int numBytesPrefetched()
    {
        return _readOffset - _startOffset + 1;
    }

    public int numBytesMarked()
    {
        return _startOffset - firstOffset();
    }

    public int totalBytes()
    {
        return _endOffset - firstOffset() + 1;
    }

    private int bufferSpaceAvailable()
    {
        assert _readOffset <= _buf.length - 1;
        return (_buf.length - 1) - _readOffset;
    }

    public byte valueAt(int offset)
    {
        assert offset >= firstOffset();
        assert offset <= _endOffset;
        return _buf[offset];
    }

    public boolean isFull()
    {
        assert totalBytes() <= _buf.length;
        return totalBytes() == _buf.length; // || windowLength() == 0 && _remainingBytes == 0
    }

    private void readBetween(int min, int max) throws IOException
    {
        assert min >= 0 && min <= max;
        assert max <= _remainingBytes;
        assert max <= bufferSpaceAvailable();

        int numBytesRead = 0;
        while (numBytesRead < min) {
            int len = _is.read(_buf, _readOffset + 1 , max - numBytesRead);
            if (len <= 0) {
                throw new EOFException(String.format("File ended prematurely " +
                                                     "(%d)", len));
            }
            numBytesRead += len;
            _readOffset += len;
            _remainingBytes -= len;
        }

        if (_log.isLoggable(Level.FINEST)) {
            _log.finest(String.format("prefetched %d bytes (min=%d, max=%d)",
                                      numBytesRead, min, max));
        }
        assert _remainingBytes >= 0;
    }


    private void readZeroes(int amount)
    {
        assert amount <= _remainingBytes;
        assert amount <= bufferSpaceAvailable();

        Arrays.fill(_buf, _readOffset + 1, _readOffset + 1 + amount, (byte)0);
        _readOffset += amount;
        _remainingBytes -= amount;
    }

    /*
     * slide window to right slideAmount number of bytes
     * _startOffset is increased by slideAmount
     * _endOffset is set to the minimum of this fileView's window size and the
     *            remaining number of bytes from the file
     * _markOffset position relative to _startOffset is left unchanged
     * _errorOffset might be set if an IO error occurred
     * _readOffset will be >= _endOffset and marks the position of the last available prefetched byte
     * read data might be compacted if there's not enough room left in the buffer
     */
    public void slide(int slideAmount)
    {
        assert slideAmount >= 0;
        assert slideAmount <= windowLength();

        if (_log.isLoggable(Level.FINEST)) {
            _log.finest(String.format("sliding %s %d", this, slideAmount));
        }

        _startOffset += slideAmount;
        int windowLength = (int) Math.min(_windowLength, numBytesPrefetched() +
                                                         _remainingBytes);
        assert windowLength >= 0;
        assert numBytesPrefetched() >= 0; // a negative value would imply a skip, which we don't (yet at least) support
        int minBytesToRead = windowLength - numBytesPrefetched();

        if (_log.isLoggable(Level.FINEST)) {
            _log.finest(String.format(
                "next window length %d, minimum bytes to read %d",
                windowLength, minBytesToRead));
        }

        if (minBytesToRead > 0) {
            if (minBytesToRead > bufferSpaceAvailable()) {
                compact();
            }

            int saveOffset = _readOffset;
            try {
                if (_ioError == null) {
                    readBetween(minBytesToRead,
                                (int) Math.min(_remainingBytes,
                                               bufferSpaceAvailable()));
                } else {
                    readZeroes(minBytesToRead);
                }
            } catch (ClosedByInterruptException e) {
                throw new RuntimeInterruptException(e);
            } catch (IOException e) {
                _ioError = e;
                int numBytesRead = _readOffset - saveOffset;
                readZeroes(minBytesToRead - numBytesRead);
            }
        }

        _endOffset = _startOffset + windowLength - 1;

        assert windowLength() == windowLength;
        assert _endOffset <= _readOffset;
    }

    private void compact()
    {
        assert numBytesPrefetched() >= 0;
        assert totalBytes() >= 0; // unless we'd support skipping

        int shiftOffset = firstOffset();
        int numShifts = numBytesMarked() + numBytesPrefetched();

        if (_log.isLoggable(Level.FINEST)) {
            _log.finest(String.format(
                "compact of %s before - buf[%d] %d bytes to buf[0], " +
                "buf.length = %d",
                this, shiftOffset, numShifts, _buf.length));
        }

        System.arraycopy(_buf, shiftOffset, _buf, 0, numShifts);
        _startOffset -= shiftOffset;
        _endOffset -= shiftOffset;
        _readOffset -= shiftOffset;
        if (_markOffset >= 0) {
            _markOffset -= shiftOffset;
        }

        assert _startOffset >= 0;
        assert _endOffset >= -1;
        assert _readOffset >= -1;
        assert _markOffset >= -1;

        if (_log.isLoggable(Level.FINEST)) {
            _log.finest(String.format("compacted %d bytes, result after: %s",
                                      numShifts, this));
        }
    }
}
