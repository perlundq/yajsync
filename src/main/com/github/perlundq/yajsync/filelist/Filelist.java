/*
 * Rsync file information list
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
package com.github.perlundq.yajsync.filelist;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.util.Environment;

public class Filelist
{
    public static class SegmentBuilder
    {
        private FileInfo _directory;
        private List<FileInfo> _files = new LinkedList<>();
        private List<FileInfo> _directories = new LinkedList<>();

        public SegmentBuilder(FileInfo directory)
        {
            _directory = directory;
        }

        @Override
        public String toString()
        {
            return String.format("%s (directory=%s, stubDirectories=%s, " +
                                 "files=%s)%n",
                                 getClass().getSimpleName(), _directory,
                                 _directories, _files);
        }

        /**
         * @throws IllegalStateException if the path of fileInfo is not below
         *         segment directory path
         */
        public void add(FileInfo fileInfo)
        {
            assert _files != null && _directories != null;
            assert fileInfo != null;
            assert !fileInfo.attrs().isOther();

            if (_directory != null && fileInfo.path() != null &&
                !fileInfo.path().startsWith(_directory.path()))
            {
                throw new IllegalStateException(String.format(
                    "%s should be a path prefix to: %s",
                    _directory.path(), fileInfo.path()));
            }
            _files.add(fileInfo);
            // NOTE: we store the directory in the builder regardless if we're
            // using recursive transfer or not
            // NOTE: we must also store DOT_DIR since this is what a native
            // sender does
            if (fileInfo.attrs().isDirectory()) {
                _directories.add(fileInfo);
            }
        }

        public int size()
        {
            return _files.size();
        }

        // O(n)
        public boolean contains(FileInfo fileInfo)
        {
            return _files.contains(fileInfo);
        }

        private void clear()
        {
            _directory = null;
            _files = null;
            _directories = null;
        }
    }

    public static class Segment implements Comparable<Integer>
    {
        private final FileInfo _directory;
        private final int _dirIndex;
        private final int _endIndex;
        private final Map<Integer, FileInfo> _files;
        private long _totalFileSize;

        private Segment(FileInfo directory, int dirIndex, List<FileInfo> files,
                        Map<Integer, FileInfo> map)
        {
            assert dirIndex >= -1;
            assert files != null;
            assert map != null;
            _directory = directory;            // NOTE: might be null
            _dirIndex = dirIndex;
            _endIndex = dirIndex + files.size();
            _files = map;

            int index = dirIndex + 1;
            Collections.sort(files);
            FileInfo prev = null;

            for (FileInfo f : files) {
                if (f.equals(prev)) {
                    if (_log.isLoggable(Level.WARNING)) {
                        @SuppressWarnings("null")       // files cannot contain null refs 
                        Path prevPath = prev.path();
                        _log.warning(String.format("skipping \"%s\" - a " +
                                                   "duplicate of \"%s\" " +
                                                   "(%s eq %s)",
                                                   f, prev, f.path(),
                                                   prevPath));
                    }
                } else {
                    _files.put(index, f);
                    if (f.attrs().isRegularFile() ||
                        f.attrs().isSymbolicLink())
                    {
                        _totalFileSize += f.attrs().size();
                    }
                }
                index++;
                prev = f;
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            int active = _files.values().size();
            int size = _endIndex - _dirIndex;
            sb.append(String.format(
                "%s [%s, dirIndex=%d, fileIndices=%d:%d, size=%d/%d]",
                getClass().getSimpleName(),
                _directory != null ? _directory.path() : "-",
                _dirIndex,
                _dirIndex + 1,
                _endIndex,
                active,
                size));
            
            if (_log.isLoggable(Level.FINEST)) {
                for (Map.Entry<Integer, FileInfo> e : _files.entrySet()) {
                    sb.append("   ").
                    append(e.getValue().path()).
                    append(", ").
                    append(e.getKey()).
                    append(Environment.PATH_SEPARATOR);
                }
            }

            return sb.toString();
        }
        
        // Collections.binarySearch
        @Override
        public int compareTo(Integer other)
        {
            return directoryIndex() - other;
        }

        // generator
        public FileInfo directory()
        {
            return _directory;
        }

        // generator
        public int directoryIndex()
        {
            return _dirIndex;
        }

        // generator sender
        public Iterable<FileInfo> files()
        {
            return _files.values();
        }

        // generator
        // can be automatically generated or possible removed
        public Iterable<Entry<Integer, FileInfo>> entrySet()
        {
            return _files.entrySet();
        }

        // generator sender receiver
        public FileInfo getFileWithIndexOrNull(int index)
        {
            assert index >= 0;
            return _files.get(index);
        }

        // sender generator
        // use bitmap
        public FileInfo remove(int index)
        {
            return _files.remove(index);
        }

        // generator
        // use bitmap
        public void removeAll()
        {
            _files.clear();
        }

        // generator
        // inefficient, can possibly be removed
        public void removeAll(Collection<Integer> toRemove)
        {
            for (int i : toRemove) {
                _files.remove(i);
            }
        }

        // sender generator
        // use bitmap
        public boolean isFinished()
        {
            return _files.isEmpty();
        }

        // use bitmap
        private boolean contains(int index)
        {
            return _files.containsKey(index);
        }
    }

    public static final int DONE   = -1;   // done with segment, may be deleted
    public static final int EOF    = -2;   // no more segments in file list
    public static final int OFFSET = -101;
    public static final Logger _log =
        Logger.getLogger(Filelist.class.getName());
    
    protected final List<Segment> _segments;
    private final boolean _isRecursive;
    private final SortedMap<Integer, FileInfo> _stubDirectories;
    private int _nextDirIndex;
    private int _stubDirectoryIndex = 0;
    private long _totalFileSize;
    private int _numFiles;

    protected Filelist(boolean isRecursive, List<Segment> segments)
    {
        _segments = segments;   // NOTE: should be an ArrayList to ensure fast binarySearch
        _isRecursive = isRecursive;
        if (isRecursive) {
            _stubDirectories = new TreeMap<>();
            _nextDirIndex = 0;
        } else {
            _stubDirectories = null;
            _nextDirIndex = -1;
        }
    }

    public Filelist(boolean isRecursive)
    {
        this(isRecursive, new ArrayList<Segment>());
    }

    protected Segment newSegment(SegmentBuilder builder,
                                 SortedMap<Integer, FileInfo> map)
    {
        assert (builder._directory == null) ==
                   (_isRecursive && _nextDirIndex == 0 ||
                    !_isRecursive && _nextDirIndex == -1);
        assert builder._directories != null;
        assert builder._files != null;

        if (_log.isLoggable(Level.FINER)) {
            _log.finer(String.format(
                "creating new segment from builder=%s and map=%s",
                builder, map));
        }

        if (_isRecursive) {
            extractStubDirectories(builder._directories);
        }
        Segment segment = new Segment(builder._directory,
                                      _nextDirIndex,
                                      builder._files,
                                      map);
        builder.clear();
        _nextDirIndex = segment._endIndex + 1;
        _segments.add(segment);
        _totalFileSize += segment._totalFileSize;
        _numFiles += segment._files.size();
        return segment;
    }

    private void extractStubDirectories(List<FileInfo> directories)
    {
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("extracting all stub directories from " + directories);
        }

        Collections.sort(directories);
        for (FileInfo f : directories) {
            assert f.attrs().isDirectory();
            if (!f.isDotDir()) {
                if (_log.isLoggable(Level.FINER)) {
                    _log.finer(String.format(
                        "adding non dot dir %s with index=%d to stub " +
                        "directories for later expansion",
                        f, _stubDirectoryIndex));
                }
                _stubDirectories.put(_stubDirectoryIndex, f);
            }
            _stubDirectoryIndex++;
        }
    }

    public Segment newSegment(SegmentBuilder builder)
    {
        return newSegment(builder, new TreeMap<Integer, FileInfo>());
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s isExpandable=%s (",
                                getClass().getSimpleName(),
                                isExpandable()));

        for (Segment s : _segments) {
            String pathName = s._directory == null ? "-"
                                                   : s._directory.path().toString();
            sb.append(", ").
               append(String.format("segment(%d, %s)",
                                    s.directoryIndex(),
                                    pathName));
        }
        sb.append(")\n").
           append("unexpanded: ").
           append(_stubDirectories);
        return sb.toString();
    }

    // NOTE: fileIndex may be directoryIndex too
    // sender receiver generator
    public Segment getSegmentWith(int index)
    {
        assert index >= 0;

        int result = Collections.binarySearch(_segments, index);
        if (result >= 0) {
            return _segments.get(result);
        }
        int insertionPoint = - result - 1;
        int segmentIndex = insertionPoint - 1;
        if (segmentIndex < 0) {
            return null;
        }
        Segment segment = _segments.get(segmentIndex);
        return segment.contains(index) ? segment : null;
    }

    // sender receiver
    /**
     * @throws RuntimeException if directoryIndex not in range
     */
    public FileInfo getStubDirectoryOrNull(int directoryIndex)
    {
        if (directoryIndex < _stubDirectories.firstKey() ||
            directoryIndex > _stubDirectories.lastKey()) {
            throw new RuntimeException(String.format("%d not within [%d:%d] (%s)",
                                                     directoryIndex,
                                                     _stubDirectories.firstKey(),
                                                     _stubDirectories.lastKey(),
                                                     this));
        }
        return _stubDirectories.remove(directoryIndex);
    }

    // sender receiver
    public boolean isExpandable()
    {
        return _stubDirectories != null && _stubDirectories.size() > 0;
    }

    // sender receiver generator
    public Segment deleteFirstSegment()
    {
        return _segments.remove(0);
    }

    // sender receiver
    public boolean isEmpty()
    {
        return _segments.isEmpty();
    }
    
    public long totalFileSize()
    {
        return _totalFileSize;
    }
    
    public int numFiles()
    {
        return _numFiles;
    }
}
