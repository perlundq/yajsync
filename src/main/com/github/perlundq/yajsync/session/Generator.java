/*
 * Receiver -> Sender communication, generation of file meta data +
 * checksum info to peer Sender
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013-2015 Per Lundqvist
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
package com.github.perlundq.yajsync.session;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.channels.Message;
import com.github.perlundq.yajsync.channels.MessageCode;
import com.github.perlundq.yajsync.channels.RsyncOutChannel;
import com.github.perlundq.yajsync.filelist.ConcurrentFilelist;
import com.github.perlundq.yajsync.filelist.FileInfo;
import com.github.perlundq.yajsync.filelist.Filelist;
import com.github.perlundq.yajsync.filelist.RsyncFileAttributes;
import com.github.perlundq.yajsync.io.FileView;
import com.github.perlundq.yajsync.io.FileViewOpenFailed;
import com.github.perlundq.yajsync.io.FileViewReadError;
import com.github.perlundq.yajsync.text.TextConversionException;
import com.github.perlundq.yajsync.text.TextDecoder;
import com.github.perlundq.yajsync.text.TextEncoder;
import com.github.perlundq.yajsync.util.FileOps;
import com.github.perlundq.yajsync.util.MD5;
import com.github.perlundq.yajsync.util.Rolling;
import com.github.perlundq.yajsync.util.RuntimeInterruptException;
import com.github.perlundq.yajsync.util.Util;

public class Generator implements RsyncTask
{
    public static class Builder
    {
        private final WritableByteChannel _out;
        private final byte[] _checksumSeed;
        private boolean _isAlwaysItemize;
        private boolean _isIgnoreTimes;
        private boolean _isInterruptible = true;
        private boolean _isListOnly;
        private boolean _isPreservePermissions;
        private boolean _isPreserveTimes;
        private boolean _isPreserveUser;
        private Charset _charset;
        private FileSelection _fileSelection = FileSelection.EXACT;
        private PrintStream _stdout = System.out;

        public Builder(WritableByteChannel out, byte[] checksumSeed)
        {
            assert out != null;
            assert checksumSeed != null;
            _out = out;
            _checksumSeed = checksumSeed;
        }

        public Builder isAlwaysItemize(boolean isAlwaysItemize)
        {
            _isAlwaysItemize = isAlwaysItemize;
            return this;
        }

        public Builder isIgnoreTimes(boolean isIgnoreTimes)
        {
            _isIgnoreTimes = isIgnoreTimes;
            return this;
        }

        public Builder isInterruptible(boolean isInterruptible)
        {
            _isInterruptible = isInterruptible;
            return this;
        }

        public Builder isListOnly(boolean isListOnly)
        {
            _isListOnly = isListOnly;
            return this;
        }

        public Builder isPreservePermissions(boolean isPreservePermissions)
        {
            _isPreservePermissions = isPreservePermissions;
            return this;
        }

        public Builder isPreserveTimes(boolean isPreserveTimes)
        {
            _isPreserveTimes = isPreserveTimes;
            return this;
        }

        public Builder isPreserveUser(boolean isPreserveUser)
        {
            _isPreserveUser = isPreserveUser;
            return this;
        }

        public Builder charset(Charset charset)
        {
            assert charset != null;
            _charset = charset;
            return this;
        }

        public Builder fileSelection(FileSelection fileSelection)
        {
            assert fileSelection != null;
            _fileSelection = fileSelection;
            return this;
        }

        public Builder stdout(PrintStream stdout)
        {
            assert stdout != null;
            _stdout = stdout;
            return this;
        }

        public Generator build()
        {
            return new Generator(this);
        }
    }

    private interface Job {
        void process() throws ChannelException;
    }

    private static final Checksum.Header ZERO_SUM;
    private static final int MIN_BLOCK_SIZE = 512;                              // TODO: make block size configurable
    private static final int OUTPUT_CHANNEL_BUF_SIZE = 8 * 1024;
    private static final Logger _log =
        Logger.getLogger(Generator.class.getName());

    private final boolean _isAlwaysItemize;
    private final boolean _isIgnoreTimes;
    private final boolean _isInterruptible;
    private final boolean _isListOnly;
    private final boolean _isPreservePermissions;
    private final boolean _isPreserveTimes;
    private final boolean _isPreserveUser;
    private final byte[] _checksumSeed;
    private final Deque<Runnable> _deferredFileAttrUpdates = new ArrayDeque<>();
    private final Filelist _fileList;
    private final FileSelection _fileSelection;
    private final LinkedBlockingQueue<Job> _jobs = new LinkedBlockingQueue<>();
    private final List<Filelist.Segment> _generated = new LinkedList<>();
    private final PrintStream _stdout;
    private final RsyncOutChannel _senderOutChannel;
    private final SimpleDateFormat _compatibleTimeFormatter =
        new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private final TextDecoder _characterDecoder;
    private final TextEncoder _characterEncoder;

    private boolean _isRunning = true;
    private int _returnStatus;

    static {
        try {
            ZERO_SUM = new Checksum.Header(0,0,0);
        } catch (Checksum.ChunkOverflow e) {
            throw new RuntimeException(e);
        }
    }

    private Generator(Builder builder)
    {
        _checksumSeed = builder._checksumSeed;
        _fileSelection = builder._fileSelection;
        _fileList = new ConcurrentFilelist(_fileSelection == FileSelection.RECURSE);
        _stdout = builder._stdout;
        _senderOutChannel = new RsyncOutChannel(builder._out,
                                                OUTPUT_CHANNEL_BUF_SIZE);
        _characterDecoder = TextDecoder.newStrict(builder._charset);
        _characterEncoder = TextEncoder.newStrict(builder._charset);
        _isAlwaysItemize = builder._isAlwaysItemize;
        _isIgnoreTimes = builder._isIgnoreTimes;
        _isInterruptible = builder._isInterruptible;
        _isListOnly = builder._isListOnly;
        _isPreservePermissions = builder._isPreservePermissions;
        _isPreserveTimes = builder._isPreserveTimes;
        _isPreserveUser = builder._isPreserveUser;
    }

    @Override
    public boolean isInterruptible()
    {
        return _isInterruptible;
    }

    @Override
    public void closeChannel() throws ChannelException
    {
        _senderOutChannel.close();
    }

    public boolean isListOnly()
    {
        return _isListOnly;
    }

    public boolean isPreservePermissions()
    {
        return _isPreservePermissions;
    }

    public boolean isPreserveTimes()
    {
        return _isPreserveTimes;
    }

    public boolean isPreserveUser()
    {
        return _isPreserveUser;
    }

    public Charset charset()
    {
        return _characterEncoder.charset();
    }

    public FileSelection fileSelection()
    {
        return _fileSelection;
    }


    public Filelist fileList()
    {
        return _fileList;
    }

    public void processJobQueueImmediate()
        throws ChannelException, InterruptedException
    {
        while (!Thread.currentThread().isInterrupted()) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("(Generator) awaiting next job...");
            }

            Job job = _jobs.take();

            if (_log.isLoggable(Level.FINE)) {
                _log.fine("(Generator) processing " + job);
            }

            job.process();
            _senderOutChannel.flush();
        }
    }

    public void processJobQueue() throws ChannelException, InterruptedException
    {
        while (!Thread.currentThread().isInterrupted()) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("(Generator) awaiting next job...");
            }

            Job job = _jobs.take();

            if (_log.isLoggable(Level.FINE)) {
                _log.fine("(Generator) processing " + job);
            }

            job.process();
            if (_jobs.isEmpty()) {
                _senderOutChannel.flush();
            }
        }
    }

    public void processJobQueueBatched() throws ChannelException, InterruptedException
    {
        List<Job> jobList = new LinkedList<>();
        while (_isRunning) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("(Generator) awaiting next jobs...");
            }

            jobList.add(_jobs.take());
            _jobs.drainTo(jobList);

            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("(Generator) got %d job(s)",
                                        jobList.size()));
            }

            for (Job job : jobList) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("(Generator) processing " + job);
                }
                job.process();
            }
            jobList.clear();
            if (_jobs.isEmpty()) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                        "(Generator) flushing %d bytes",
                        _senderOutChannel.numBytesBuffered()));
                }
                _senderOutChannel.flush();
            }
        }
    }

    @Override
    public Boolean call() throws ChannelException, InterruptedException
    {
        try {
            processJobQueueBatched();
            return _returnStatus == 0;
        } catch (RuntimeInterruptException e) {
            throw new InterruptedException();
        }
    }

    public void purgeFile(final Filelist.Segment segment, final int index)
        throws InterruptedException
    {
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                if (segment != null) {
                    segment.remove(index);
                } else {
                    Filelist.Segment tmpSegment = _fileList.getSegmentWith(index);
                    if (tmpSegment == null) {
                        throw new RsyncProtocolException(String.format(
                            "invalid file index %d from peer", index));
                    }
                    tmpSegment.remove(index);
                }
                removeAllFinishedSegmentsAndNotifySender();
            }

            @Override
            public String toString() {
                return String.format("purgeFile(%s, %d)", segment, index);
            }
        };
        appendJob(j);
    }

    public void stop() throws InterruptedException
    {
        Job j = new Job() {
            @Override
            public void process() {
                for (Runnable r : _deferredFileAttrUpdates) {
                    r.run();
                }
                _isRunning = false;
            }
            @Override
            public String toString() {
                return "stop()";
            }
        };
        appendJob(j);
    }

    // used for sending empty filter rules only
    public void sendBytes(final ByteBuffer buf) throws InterruptedException
    {
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                _senderOutChannel.put(buf);
            }

            @Override
            public String toString() {
                return String.format("sendBytes(%s)", buf.duplicate());
            }
        };
        appendJob(j);
    }

    /**
     * @throws TextConversionException
     */
    public void sendMessage(final MessageCode code, final String text)
        throws InterruptedException
    {
        final ByteBuffer payload =
            ByteBuffer.wrap(_characterEncoder.encode(text));
        final Message message = new Message(code, payload);

        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                _senderOutChannel.putMessage(message);
            }

            @Override
            public String toString() {
                return String.format("sendMessage(%s, %s)", code, text);
            }
        };
        appendJob(j);
    }

    public void generateSegment(final Filelist.Segment segment)
        throws InterruptedException
    {
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                if (_isListOnly) {
                    if (_fileSelection != FileSelection.RECURSE) {
                        listFullSegment(segment);
                    } else if (segment.directory() == null) {
                        listInitialSegmentRecursive(segment);
                    } else {
                        listSegmentRecursive(segment);
                    }
                    segment.removeAll();
                } else {
                    sendChecksumForSegment(segment);
                }
                _generated.add(segment);
                removeAllFinishedSegmentsAndNotifySender();
            }

            @Override
            public String toString() {
                return String.format("generateSegment(%s)", segment);
            }
        };
        appendJob(j);
    }

    public void generateFile(final Filelist.Segment segment,
                             final int fileIndex,
                             final FileInfo fileInfo)
        throws InterruptedException
    {
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                try {
                    boolean isTransfer =
                        sendFileMetadata(fileIndex, fileInfo,
                                          Checksum.MAX_DIGEST_LENGTH);
                    if (!isTransfer) {
                        segment.remove(fileIndex);
                        removeAllFinishedSegmentsAndNotifySender();
                    }
                } catch (IOException e) { // sendFileMetadata
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(String.format(
                            "(Generator) failed to generate file meta data " +
                            "for %s (index %d): %s",
                            fileInfo.path(), fileIndex, e.getMessage()));
                    }
                    _returnStatus++;
                }
            }

            @Override
            public String toString()
            {
                return String.format("generateFile (%s, %d, %s)",
                                     segment, fileIndex, fileInfo.path());
            }
        };
        appendJob(j);
    }

    public void sendSegmentDone() throws InterruptedException
    {
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                _senderOutChannel.encodeIndex(Filelist.DONE);
            }

            @Override
            public String toString()
            {
                return "sendSegmentDone()";
            }
        };
        appendJob(j);
    }

    private void appendJob(Job job) throws InterruptedException
    {
        assert job != null;
        _jobs.put(job);
    }

    // NOTE: no error if dir already exists
    private void mkdir(FileInfo dir) throws IOException
    {
        if (_log.isLoggable(Level.FINE)) {
            _log.fine("(Generator) creating directory " + dir.path());
        }
        RsyncFileAttributes attrs = RsyncFileAttributes.statOrNull(dir.path());
        if (attrs == null) {
            Files.createDirectories(dir.path());
        }
        deferUpdateAttrsIfDiffer(dir.path(), attrs, dir.attrs());
    }

    private int sendChecksumForSegmentFiles(Filelist.Segment segment)
        throws ChannelException
    {
        int numErrors = 0;
        List<Integer> toRemove = new LinkedList<>();

        for (Map.Entry<Integer, FileInfo> entry : segment.entrySet()) {
            final int index = entry.getKey();
            final FileInfo f = entry.getValue();
            boolean isTransfer = false;
            try {
                if (f.isTransferrable()) {
                    if (f.attrs().isRegularFile()) {
                        isTransfer = sendFileMetadata(
                                         index,
                                         f,
                                         Checksum.MIN_DIGEST_LENGTH);
                    } else if (_fileSelection != FileSelection.RECURSE &&
                               f.attrs().isDirectory()) {
                        sendDirectoryMetadata(index, f);
                    } else {
                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("(Generator) Skipping " + f.path());
                        }
                    }
                }
            } catch (IOException e) {
                if (f.attrs().isDirectory()) {
                    f.prune(); // we cannot remove the corresponding segment since we may not have received it yet
                }
                if (_log.isLoggable(Level.WARNING)) {
                    _log.warning(String.format(
                        "(Generator) failed to generate file %s (index %d): %s",
                        f.path(), index, e.getMessage()));
                }
                numErrors++;
            }
            if (!isTransfer) {
                toRemove.add(index);
            }
        }
        segment.removeAll(toRemove);
        return numErrors;
    }

    // TODO: print symbolic link target
    private String listFileInfo(FileInfo f)
    {
        // mode size date name
        RsyncFileAttributes attrs = f.attrs();
        String pathName = _characterDecoder.decodeOrNull(f.pathNameBytes());
        if (pathName == null) { // or should we just silently skip it?
            pathName = String.format("%s <WARNING filename contains " +
                                     "undecodable characters (using %s)>",
                                     new String(f.pathNameBytes(),
                                                _characterDecoder.charset()),
                                     _characterDecoder.charset());
        }
        return String.format("%s %11d %s %s",
                             FileOps.modeToString(attrs.mode()),
                             attrs.size(),
                             _compatibleTimeFormatter.format(new Date(FileTime.from(attrs.lastModifiedTime(), TimeUnit.SECONDS).toMillis())),
                             pathName);
//                                 FileTime.from(attrs.lastModifiedTime(),
//                                           TimeUnit.SECONDS),
    }

    private void listFullSegment(Filelist.Segment segment)
    {
        assert _fileSelection != FileSelection.RECURSE;
        assert segment.directory() == null;
        for (FileInfo f : segment.files()) {
            _stdout.println(listFileInfo(f));
        }
    }

    private void listInitialSegmentRecursive(Filelist.Segment segment)
    {
        assert _fileSelection == FileSelection.RECURSE;
        assert segment.directory() == null;
        boolean listFirstDotDir = true;
        for (FileInfo f : segment.files()) {
            if (!f.attrs().isDirectory()) {
                _stdout.println(listFileInfo(f));
            } else if (listFirstDotDir) {
                if (f.isDotDir()) {
                    _stdout.println(listFileInfo(f));
                }
                listFirstDotDir = false;
            }
        }
    }

    private void listSegmentRecursive(Filelist.Segment segment)
    {
        assert _fileSelection == FileSelection.RECURSE;
        assert segment.directory() != null;
        _stdout.println(listFileInfo(segment.directory()));
        for (FileInfo f : segment.files()) {
            if (!f.attrs().isDirectory()) {
                _stdout.println(listFileInfo(f));
            }
        }
    }

    private void sendChecksumForSegment(Filelist.Segment segment)
        throws ChannelException
    {
        assert !_isListOnly;
        assert segment != null;

        final int dirIndex = segment.directoryIndex();
        FileInfo dir = segment.directory();
        if (dir != null && (dir.isPruned() || !dir.isTransferrable())) {
            segment.removeAll();
            return;
        }

        boolean isInitialFileList = dir == null;
        if (isInitialFileList) {
            dir = segment.getFileWithIndexOrNull(dirIndex + 1);
        }
        if (dir == null) { // initial file list is empty
            return;
        }

        try {
            if (dir.attrs().isDirectory()) {
                mkdir(dir);
            }
            if (!isInitialFileList) {
                sendDirectoryMetadata(dirIndex, dir);
            }
            _returnStatus += sendChecksumForSegmentFiles(segment);
        } catch (IOException e) {
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning(String.format(
                    "(Generator) failed to generate files below dir %s " +
                    "(index %d): %s",
                    dir.path(), dirIndex, e.getMessage()));
            }
            segment.removeAll();
            _returnStatus++;
        }
    }

    private static boolean isDataModified(RsyncFileAttributes old,
                                          RsyncFileAttributes current)
    {
        assert old != null;
        return current == null ||
               old.size() != current.size() ||
               old.lastModifiedTime() != current.lastModifiedTime();
    }

    private void sendDirectoryMetadata(int index, FileInfo fileInfo)
        throws ChannelException,IOException
    {
        assert index >= 0;
        assert fileInfo != null && fileInfo.attrs().isDirectory();

        if (_log.isLoggable(Level.FINE)) {
            _log.fine("(Generator) generating directory " + fileInfo.path());
        }

        RsyncFileAttributes existingAttrs =
            RsyncFileAttributes.statIfExists(fileInfo.path());                  // value: null if file does not exist else non-null, throws IOException for other errors
        boolean isRemoved = removeExistingIfDifferentType(fileInfo, existingAttrs); // throws IOException if fails to remove existing
        if (isRemoved) {
            existingAttrs = null;
        }

        itemizeDirectory(index, fileInfo, existingAttrs);
    }

    private boolean sendFileMetadata(int index,
                                     FileInfo fileInfo,
                                     int digestLength)
        throws ChannelException,IOException
    {
        assert index >= 0;
        assert fileInfo != null && fileInfo.attrs().isRegularFile();

        if (_log.isLoggable(Level.FINE)) {
            _log.fine(String.format("(Generator) generating file %s, index %d",
                                    fileInfo.path(), index));
        }

        RsyncFileAttributes existingAttrs =
            RsyncFileAttributes.statIfExists(fileInfo.path());              // value: null if file does not exist else non-null, throws IOException for other errors
        boolean isRemoved = removeExistingIfDifferentType(fileInfo, existingAttrs); // throws IOException if fails to remove existing
        if (isRemoved) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("(Generator) removed existing file of different " +
                          "type " + existingAttrs);
            }
            existingAttrs = null;
        }

        if (_log.isLoggable(Level.FINE)) {
            _log.fine("(Generator) existing attrs=" + existingAttrs);
            _log.fine("(Generator) target attrs=" + fileInfo.attrs());
        }

        return itemizeFile(index, fileInfo, existingAttrs, digestLength);
    }

    private void sendChecksumHeader(Checksum.Header header)
        throws ChannelException
    {
        Connection.sendChecksumHeader(_senderOutChannel, header);
    }

    // could we possibly adaptively correlate block checksum size with checksum
    // match ratio? or inversely correlate channel speed or a combination
    private int getBlockLengthFor(long fileSize)
    {
        assert fileSize >= 0;
        if (fileSize == 0) {
            return 0;
        }
        int blockLength = pow2SquareRoot(fileSize);
        assert fileSize / blockLength <= Integer.MAX_VALUE;
        return Math.max(MIN_BLOCK_SIZE, blockLength);
    }

    // reduce protocol overhead when sending lots of checksums
    private static int getDigestLength(long fileSize, int block_length)
    {
        int result = ((int) (10 + 2 * (long) Util.log2(fileSize) -
                            (long) Util.log2(block_length)) - 24) / 8;
        result = Math.min(result, Checksum.MAX_DIGEST_LENGTH);
        return Math.max(result, Checksum.MIN_DIGEST_LENGTH);
    }

    private void sendItemizeAndChecksums(int index,
                                         FileInfo fileInfo,
                                         RsyncFileAttributes curAttrs,
                                         int minDigestLength)
        throws ChannelException
    {
        long currentSize = curAttrs.size();
        int blockLength = getBlockLengthFor(currentSize);
//        int blockLength = getCompatibleBlockLengthFor(currentSize);
        int windowLength = blockLength;
        int digestLength = currentSize > 0
                           ? Math.max(minDigestLength,
                                      getDigestLength(currentSize, blockLength))
                           : 0;

        try (FileView fv = new FileView(fileInfo.path(),
                                        currentSize,
                                        blockLength,
                                        windowLength)) {   // throws FileViewOpenFailed

            Checksum.Header header = new Checksum.Header(blockLength,
                                                         digestLength,
                                                         currentSize); // throws ChunkCountOverflow
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("(Generator) generating file %s, " +
                                        "index %d, checksum %s",
                                        fileInfo, index, header));
            }

            sendItemizeInfo(index, curAttrs, fileInfo.attrs(), Item.TRANSFER);
            sendChecksumHeader(header);

            MessageDigest md = MD5.newInstance();

            while (fv.windowLength() > 0) {
                int rolling = Rolling.compute(fv.array(),
                                              fv.startOffset(),
                                              fv.windowLength());
                _senderOutChannel.putInt(rolling);
                md.update(fv.array(), fv.startOffset(), fv.windowLength());
                md.update(_checksumSeed);
                byte[] md5 = md.digest();
                _senderOutChannel.put(md5, 0, digestLength);
                fv.slide(fv.windowLength());
            }
        } catch (FileViewOpenFailed | Checksum.ChunkOverflow e) {
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning(String.format(
                    "(Generator) received I/O error during checksum " +
                    "generation (%s)", e.getMessage()));
            }
            sendItemizeInfo(index, null, fileInfo.attrs(), Item.TRANSFER);
            sendChecksumHeader(ZERO_SUM);
        } catch (FileViewReadError e) { // from FileView.close() if there were any I/O errors during file read
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning("(Generator) Warning got I/O errors during " +
                             "checksum generation. Errors ignored and data " +
                             "filled with zeroes): " + e.getMessage());
            }
        }
    }

    private void updateAttrsIfDiffer(Path path, RsyncFileAttributes curAttrs,
                                     RsyncFileAttributes targetAttrs)
        throws IOException
    {
        if (_isPreservePermissions && (curAttrs == null ||
                                       curAttrs.mode() != targetAttrs.mode())) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                    "(Generator) updating file permissions %o -> %o on %s",
                    curAttrs == null ? 0 : curAttrs.mode(),
                    targetAttrs.mode(), path));
            }
            FileOps.setFileMode(path, targetAttrs.mode(),
                                LinkOption.NOFOLLOW_LINKS);
        }
        if (_isPreserveTimes &&
            (curAttrs == null ||
             curAttrs.lastModifiedTime() != targetAttrs.lastModifiedTime()))
        {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                    "(Generator) updating mtime %d -> %d on %s",
                    curAttrs == null ? 0 : curAttrs.lastModifiedTime(),
                    targetAttrs.lastModifiedTime(), path));
            }
            FileOps.setLastModifiedTime(path, targetAttrs.lastModifiedTime(),
                                        LinkOption.NOFOLLOW_LINKS);
        }
        // NOTE: keep this one last in the method, in case we fail due to
        //       insufficient permissions (the other ones are more likely to
        //       succeed).
        // NOTE: we cannot detect if we have the capabilities to change
        //       ownership (knowing if UID 0 is not sufficient)
        // TODO: fall back to changing uid (find out how rsync works) if name
        //       change fails
        if (_isPreserveUser && !targetAttrs.user().name().isEmpty() &&
            (curAttrs == null ||
             !curAttrs.user().name().equals(targetAttrs.user().name())))
        {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                    "(Generator) updating ownership %s -> %s on %s",
                    curAttrs == null ? "" : curAttrs.user(),
                    targetAttrs.user(), path));
            }
            // NOTE: side effect of chown in Linux is that set user/group id bit
            //       might be cleared.
            FileOps.setOwner(path, targetAttrs.user(),
                             LinkOption.NOFOLLOW_LINKS);
        } else if (_isPreserveUser && targetAttrs.user().name().isEmpty() &&
            (curAttrs == null ||
             curAttrs.user().uid() != targetAttrs.user().uid()))
        {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                    "(Generator) updating uid %s -> %d on %s",
                    curAttrs == null ? "" : curAttrs.user().uid(),
                    targetAttrs.user().uid(), path));
            }
            // NOTE: side effect of chown in Linux is that set user/group id bit
            //       might be cleared.
            FileOps.setUserId(path, targetAttrs.user().uid(),
                              LinkOption.NOFOLLOW_LINKS);
        }
    }

    private void deferUpdateAttrsIfDiffer(final Path path,
                                          final RsyncFileAttributes curAttrs,
                                          final RsyncFileAttributes targetAttrs)
    {
        Runnable j = new Runnable() {
            @Override
            public void run() {
                try {
                    updateAttrsIfDiffer(path, curAttrs, targetAttrs);
                } catch (IOException e) {
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(String.format(
                            "(Generator) received I/O error while applying " +
                            "attributes on %s: %s", path, e.getMessage()));
                    }
                    _returnStatus++;
                }
            }
        };
        _deferredFileAttrUpdates.addFirst(j);
    }

    private boolean itemizeFile(int index,
                                FileInfo fileInfo,
                                RsyncFileAttributes curAttrs,
                                int digestLength)
        throws ChannelException
    {
        // NOTE: native opens the file first though even if its file size is zero
        if (isDataModified(fileInfo.attrs(), curAttrs) || _isIgnoreTimes) {
            if (curAttrs == null) {
                sendItemizeInfo(index, curAttrs, fileInfo.attrs(),
                                Item.TRANSFER);
                sendChecksumHeader(ZERO_SUM);
            } else {
                sendItemizeAndChecksums(index, fileInfo, curAttrs,
                                        digestLength);
            }
            return true;
        }

        if (_isAlwaysItemize) {
            sendItemizeInfo(index, curAttrs, fileInfo.attrs(), Item.NO_CHANGE);
        }

        try {
            updateAttrsIfDiffer(fileInfo.path(), curAttrs, fileInfo.attrs());
        } catch (IOException e) {
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning(String.format(
                    "(Generator) received I/O error while applying " +
                    "attributes on %s: %s", fileInfo.path(), e.getMessage()));
            }
            _returnStatus++;
        }
        return false;
    }

    private char itemizeFlags(RsyncFileAttributes curAttrs,
                              RsyncFileAttributes targetAttrs)
    {
        if (curAttrs == null) {
            return Item.IS_NEW;
        }

        char iFlags = Item.NO_CHANGE;
        if (_isPreservePermissions && curAttrs.mode() != targetAttrs.mode()) {
            iFlags |= Item.REPORT_PERMS;
        }
        if (_isPreserveTimes &&
            curAttrs.lastModifiedTime() != targetAttrs.lastModifiedTime())
        {
            iFlags |= Item.REPORT_TIME;
        }
        if (_isPreserveUser && !curAttrs.user().equals(targetAttrs.user())) {
            iFlags |= Item.REPORT_OWNER;
        }
        if (curAttrs.isRegularFile() && curAttrs.size() != targetAttrs.size()) {
            iFlags |= Item.REPORT_SIZE;
        }

        return iFlags;
    }

    private void sendItemizeInfo(int index,
                                 RsyncFileAttributes curAttrs,
                                 RsyncFileAttributes targetAttrs,
                                 char iMask)
        throws ChannelException
    {
        char iFlags = (char) (iMask | itemizeFlags(curAttrs, targetAttrs));
        if (_log.isLoggable(Level.FINE)) {
            _log.fine("(Generator) sending itemizeFlags=" + (int) iFlags);
        }
        _senderOutChannel.encodeIndex(index);
        _senderOutChannel.putChar(iFlags);
    }

    private void itemizeDirectory(int index,
                                  FileInfo fileInfo,
                                  RsyncFileAttributes curAttrs)
        throws ChannelException,IOException
    {
        if (curAttrs == null) {
            sendItemizeInfo(index, curAttrs, fileInfo.attrs(),
                            Item.LOCAL_CHANGE);
            mkdir(fileInfo);   // throws IOException
        } else {
            if (_isAlwaysItemize) {
                sendItemizeInfo(index, curAttrs, fileInfo.attrs(),
                                Item.NO_CHANGE);
            }
            deferUpdateAttrsIfDiffer(fileInfo.path(),
                                     curAttrs, fileInfo.attrs());
        }
    }

    private boolean removeExistingIfDifferentType(
                                              FileInfo fileInfo,
                                              RsyncFileAttributes existingAttrs)
        throws IOException
    {
        if (existingAttrs != null &&
            existingAttrs.fileType() != fileInfo.attrs().fileType()) {
            // TODO: BUG: this won't properly delete non-empty directories
            Files.deleteIfExists(fileInfo.path());
            return true;
        } else {
            return false;
        }
    }

    // return the square root of num as the nearest lower number in base 2
    /**
     * @throws IllegalArgumentException if num is negative or result would
     *         overflow an integer
     */
    private static int pow2SquareRoot(long num)
    {
        if (num < 0) {
            throw new IllegalArgumentException(String.format(
                "cannot compute square root of %d", num));
        }

        if (num == 0) {
            return 0;
        }
        // sqrt(2**n) == 2**(n/2)
        long nearestLowerBase2 = Long.highestOneBit(num);
        int exponent = Long.numberOfTrailingZeros(nearestLowerBase2);
        int sqrtExponent = exponent / 2;
        long result = 1 << sqrtExponent;
        if (result < 0 || result > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(
                "square root of %d (%d) is either negative or larger than max" +
                " int value (%d)", num, result, Integer.MAX_VALUE));
        }
        return (int) result;
    }

    private void removeAllFinishedSegmentsAndNotifySender()
        throws ChannelException
    {
        for (Iterator<Filelist.Segment> it = _generated.iterator(); it.hasNext(); ) {
            Filelist.Segment segment = it.next();
            if (!segment.isFinished()) {
                break;
            }
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("(Generator) removing finished " +
                                        "segment %s and sending index %d",
                                        segment, Filelist.DONE));
            }
            Filelist.Segment deleted = _fileList.deleteFirstSegment();
            if (deleted != segment) { // identity comparison
                throw new IllegalStateException(String.format("%s != %s",
                                                              deleted,
                                                              segment));
            }
            it.remove(); // NOTE: remove before notifying peer
            _senderOutChannel.encodeIndex(Filelist.DONE);
        }
    }

    public synchronized long numBytesWritten()
    {
        return _senderOutChannel.numBytesWritten();
    }
}
