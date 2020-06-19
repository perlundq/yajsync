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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.internal.util.RuntimeInterruptException;

public class FileView implements AutoCloseable
{
    private static final Logger _log =
        Logger.getLogger(FileView.class.getName());
    private final FileChannel _channel;
    private final int _windowLength;  // size of sliding window (<= _buf.length)
    private ByteBuffer _map;
    private long _mappedPosition = 0; // start position in file of the currently mapped buffer
    private final String _fileName;
    private final long _fileSize;
    private int _startOffset = 0;  // current window start offset in buffer
    private int _endOffset = 0;   // current window exclusive end offset in buffer  // window length == _endOffset - _startOffset 
    private int _markOffset = -1;
    private long _remainingBytes; // remaining bytes in file, not read yet into buffer
    private IOException _ioError = null;

    public FileView(Path path, long fileSize, int windowLength)
        throws FileViewOpenFailed
    {
        assert path != null;
        assert fileSize >= 0;
        assert windowLength >= 0;

        try {
            _fileName = path.toString();
            _fileSize= fileSize;
            _remainingBytes = fileSize;

            _map = ByteBuffer.allocate( 0 );

            if (fileSize > 0) {
                _channel = FileChannel.open(path, StandardOpenOption.READ);
                _windowLength = windowLength;
                slide(0);
                assert _startOffset == 0;
                assert _endOffset > 0;
            } else {
                _channel = null;
                _windowLength = 0;
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
                             "mapPosition=%d, remainingBytes=%d)",
                             this.getClass().getSimpleName(),
                             _fileName, _startOffset, _markOffset, _endOffset,
                             windowLength(), _mappedPosition, _remainingBytes);
    }

    @Override
    public void close() throws FileViewException
    {
        if (_channel != null) {
            try {
                _channel.close();
                _map = null;
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
    
    public ByteBuffer windowBuffer() {
        return (ByteBuffer) _map.duplicate().position( _startOffset ).limit( _endOffset );
    }
    
    public ByteBuffer markedBuffer() {
        return (ByteBuffer) _map.duplicate().position( markOffset() ).limit( _startOffset );
    }

    public ByteBuffer totalBuffer() {
        return (ByteBuffer) _map.duplicate().position( firstOffset() ).limit( _endOffset );
    }

    // TODO: the names startOffset and firstOffset are confusingly similar
    public int startOffset()
    {
        assert _startOffset >= 0;
        assert _startOffset < _map.limit() || _channel == null;
        return _startOffset;
    }

    // might return -1
    public int markOffset()
    {
        assert _markOffset >= -1;
        assert _markOffset < _map.limit() || _channel == null;
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
        assert _endOffset > 0;
        return _endOffset - 1;
    }

    public void setMarkRelativeToStart(int relativeOffset)
    {
        assert relativeOffset >= 0;
        // it's allowed to move 1 passed _endOffset, as length is defined as:
        // _endOffset - _startOffset + 1
        assert _startOffset + relativeOffset <= _endOffset;
        _markOffset = _startOffset + relativeOffset;
    }

    public int windowLength()
    {
        int length = _endOffset - _startOffset ;
        assert length >= 0;
        assert length <= _windowLength : length + " maxWindowLength=" +
                                         _windowLength;
        return length;
    }

    public int numBytesPrefetched()
    {
        return _map.limit() - _startOffset ;
    }

    public int numBytesMarked()
    {
        return _startOffset - firstOffset();
    }

    public int totalBytes()
    {
        return _endOffset - firstOffset();
    }

    public byte valueAt(int offset)
    {
        assert offset >= firstOffset();
        assert offset < _endOffset;
        return _map.get( offset );
    }

    public boolean isFull()
    {
        assert totalBytes() <= _map.limit();
        return totalBytes() == _map.limit(); // || windowLength() == 0 && _remainingBytes == 0
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
        int minBytesToRead = windowLength - numBytesPrefetched();

        if (_log.isLoggable(Level.FINEST)) {
            _log.finest(String.format(
                "next window length %d, minimum bytes to read %d",
                windowLength, minBytesToRead));
        }

        if (minBytesToRead > 0) {

            try {
                if (_ioError == null) {
                    // remap of up to 2g bytes starting with new offset.
                    // old buffer will be GCed
                    int shift = firstOffset();
                    long mapsize = Math.min( Integer.MAX_VALUE/_windowLength*_windowLength, _fileSize - _mappedPosition - shift );
                    _map = _channel.map( MapMode.READ_ONLY, _mappedPosition + shift, mapsize );

                    _mappedPosition += shift;
                    _startOffset -= shift;
                    if ( _markOffset >= 0 )
                        _markOffset -= shift;
                    _remainingBytes = _fileSize - _mappedPosition - mapsize;
                    if (_log.isLoggable(Level.FINEST)) {
                        _log.finest(String.format("remaped %s ", this));
                    }
                } else {
                    // PENDING: the idea of sending zeroes on read error instead if bailing out from sync
                    // process early seems broken
                    
                    // reuse zero filled bytebuffer, if its capacity is not less when required
                    if ( minBytesToRead > _map.capacity() ) {
                        _map = ByteBuffer.allocate( minBytesToRead );
                    }
                    _map.limit( minBytesToRead );
                    _startOffset = 0;
                    _markOffset = -1;
                    _remainingBytes -= _map.limit();
                }
            } catch (ClosedByInterruptException e) {
                throw new RuntimeInterruptException(e);
            } catch (IOException e) {
                _ioError = e;
                // PENDING: idea of writing zeroes on read error instead if bailing out from sync
                // process early seems broken

                // zero filled byte buffer
                _map = ByteBuffer.allocate( minBytesToRead );
                _startOffset = 0;
                _markOffset = -1;
                _remainingBytes -= _map.limit();
            }
        }

        _endOffset = _startOffset + windowLength ;

        assert windowLength() == windowLength;
        assert _endOffset <= _map.limit();
    }

}
