/*
 * Receiver -> Sender communication, generation of file meta data +
 * checksum info to peer Sender
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
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
package com.github.perlundq.yajsync.session;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.channels.Message;
import com.github.perlundq.yajsync.channels.MessageCode;
import com.github.perlundq.yajsync.channels.RsyncOutChannel;
import com.github.perlundq.yajsync.filelist.ConcurrentFilelist;
import com.github.perlundq.yajsync.filelist.DeviceInfo;
import com.github.perlundq.yajsync.filelist.FileInfo;
import com.github.perlundq.yajsync.filelist.Filelist;
import com.github.perlundq.yajsync.filelist.RsyncFileAttributes;
import com.github.perlundq.yajsync.filelist.SymlinkInfo;
import com.github.perlundq.yajsync.io.FileView;
import com.github.perlundq.yajsync.io.FileViewOpenFailed;
import com.github.perlundq.yajsync.io.FileViewReadError;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.text.TextConversionException;
import com.github.perlundq.yajsync.text.TextEncoder;
import com.github.perlundq.yajsync.util.FileOps;
import com.github.perlundq.yajsync.util.MD5;
import com.github.perlundq.yajsync.util.Pair;
import com.github.perlundq.yajsync.util.Rolling;
import com.github.perlundq.yajsync.util.RuntimeInterruptException;
import com.github.perlundq.yajsync.util.Util;

public class Generator implements RsyncTask, Iterable<FileInfo>
{
    public static class Builder
    {
        private final WritableByteChannel _out;
        private final byte[] _checksumSeed;
        private boolean _isAlwaysItemize;
        private boolean _isIgnoreTimes;
        private boolean _isInterruptible = true;
        private boolean _isDelete;
        private boolean _isListOnly;
        private boolean _isPreserveDevices;
        private boolean _isPreserveLinks;
        private boolean _isPreservePermissions;
        private boolean _isPreserveSpecials;
        private boolean _isPreserveTimes;
        private boolean _isPreserveUser;
        private boolean _isPreserveGroup;
        private boolean _isNumericIds;
        private Charset _charset;
        private FileSelection _fileSelection = FileSelection.EXACT;

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

        public Builder isDelete(boolean isDelete)
        {
            _isDelete = isDelete;
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

        public Builder isPreserveDevices(boolean isPreserveDevices)
        {
            _isPreserveDevices = isPreserveDevices;
            return this;
        }

        public Builder isPreserveLinks(boolean isPreserveLinks)
        {
            _isPreserveLinks = isPreserveLinks;
            return this;
        }

        public Builder isPreservePermissions(boolean isPreservePermissions)
        {
            _isPreservePermissions = isPreservePermissions;
            return this;
        }

        public Builder isPreserveSpecials(boolean isPreserveSpecials)
        {
            _isPreserveSpecials = isPreserveSpecials;
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

        public Builder isPreserveGroup(boolean isPreserveGroup)
        {
            _isPreserveGroup = isPreserveGroup;
            return this;
        }

        public Builder isNumericIds(boolean isNumericIds)
        {
            _isNumericIds = isNumericIds;
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

        public Generator build()
        {
            assert !_isDelete || _fileSelection != FileSelection.EXACT;
            return new Generator(this);
        }
    }

    private interface Job {
        void process() throws ChannelException;
    }

    private static final Checksum.Header ZERO_SUM;
    private static final int MIN_BLOCK_SIZE = 512;
    private static final int OUTPUT_CHANNEL_BUF_SIZE = 8 * 1024;
    private static final Logger _log =
        Logger.getLogger(Generator.class.getName());

    private final BitSet _pruned = new BitSet();
    private final boolean _isAlwaysItemize;
    private final boolean _isDelete;
    private final boolean _isIgnoreTimes;
    private final boolean _isInterruptible;
    private final boolean _isListOnly;
    private final boolean _isPreserveDevices;
    private final boolean _isPreserveLinks;
    private final boolean _isPreservePermissions;
    private final boolean _isPreserveSpecials;
    private final boolean _isPreserveTimes;
    private final boolean _isPreserveUser;
    private final boolean _isPreserveGroup;
    private final boolean _isNumericIds;
    private final byte[] _checksumSeed;
    private final Deque<Job> _deferredJobs = new ArrayDeque<>();
    private final Filelist _fileList;
    private final FileSelection _fileSelection;
    private final LinkedBlockingQueue<Job> _jobs = new LinkedBlockingQueue<>();
    private final BlockingQueue<Pair<Boolean, FileInfo>> _listing =
            new LinkedBlockingQueue<>();
    private final List<Filelist.Segment> _generated = new LinkedList<>();
    private final RsyncOutChannel _out;
    private final SimpleDateFormat _dateFormat = new SimpleDateFormat();
    private final TextEncoder _characterEncoder;

    private boolean _isRunning = true;
    private int _returnStatus;
    private volatile boolean _isDeletionsEnabled;

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
        _fileList =
                new ConcurrentFilelist(_fileSelection == FileSelection.RECURSE,
                                       true);
        _out = new RsyncOutChannel(builder._out, OUTPUT_CHANNEL_BUF_SIZE);
        _characterEncoder = TextEncoder.newStrict(builder._charset);
        _isAlwaysItemize = builder._isAlwaysItemize;
        _isDelete = builder._isDelete;
        _isDeletionsEnabled = _fileSelection != FileSelection.EXACT;
        _isIgnoreTimes = builder._isIgnoreTimes;
        _isInterruptible = builder._isInterruptible;
        _isListOnly = builder._isListOnly;
        _isPreserveDevices = builder._isPreserveDevices;
        _isPreserveLinks = builder._isPreserveLinks;
        _isPreservePermissions = builder._isPreservePermissions;
        _isPreserveSpecials = builder._isPreserveSpecials;
        _isPreserveTimes = builder._isPreserveTimes;
        _isPreserveUser = builder._isPreserveUser;
        _isPreserveGroup = builder._isPreserveGroup;
        _isNumericIds = builder._isNumericIds;
    }

    @Override
    public String toString()
    {
        return String.format(
                "%s(" +
                "isAlwaysItemize=%b, " +
                "isDelete=%b, " +
                "isIgnoreTimes=%b, " +
                "isInterruptible=%b, " +
                "isListOnly=%b, " +
                "isNumericIds=%b, " +
                "isPreserveDevices=%b, " +
                "isPreserveLinks=%b, " +
                "isPreservePermissions=%b, " +
                "isPreserveSpecials=%b, " +
                "isPreserveTimes=%b, " +
                "isPreserveUser=%b, " +
                "isPreserveGroup=%b, " +
                "checksumSeed=%s, " +
                "fileSelection=%s" +
                ")",
                getClass().getSimpleName(),
                _isAlwaysItemize,
                _isDelete,
                _isIgnoreTimes,
                _isInterruptible,
                _isListOnly,
                _isNumericIds,
                _isPreserveDevices,
                _isPreserveLinks,
                _isPreservePermissions,
                _isPreserveSpecials,
                _isPreserveTimes,
                _isPreserveUser,
                _isPreserveGroup,
                Text.bytesToString(_checksumSeed),
                _fileSelection);
    }

    @Override
    public boolean isInterruptible()
    {
        return _isInterruptible;
    }

    @Override
    public void closeChannel() throws ChannelException
    {
        _out.close();
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

    public boolean isPreserveGroup()
    {
        return _isPreserveGroup;
    }

    public boolean isNumericIds()
    {
        return _isNumericIds;
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

    public void processJobQueueBatched() throws ChannelException,
                                                InterruptedException
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
                        _out.numBytesBuffered()));
                }
                _out.flush();
            }
        }
    }

    @Override
    public Boolean call() throws ChannelException, InterruptedException
    {
        try {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(this.toString());
            }
            processJobQueueBatched();
            return _returnStatus == 0;
        } catch (RuntimeInterruptException e) {
            throw new InterruptedException();
        } finally {
            Pair<Boolean, FileInfo> poisonPill = new Pair<>(false, null);
            _listing.add(poisonPill);
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
                    Filelist.Segment tmpSegment =
                            _fileList.getSegmentWith(index);
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
        Job job = new Job() {
            @Override
            public void process() {
                _isRunning = false;
            }
            @Override
            public String toString() {
                return "stop()";
            }
        };
        appendJob(job);
    }

    // used for sending empty filter rules only
    public void sendBytes(final ByteBuffer buf) throws InterruptedException
    {
        assert buf != null;

        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                _out.put(buf);
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
    private Message toMessage(MessageCode code, String text)
    {
        ByteBuffer payload = ByteBuffer.wrap(_characterEncoder.encode(text));
        return new Message(code, payload);
    }

    /**
     * @throws TextConversionException
     */
    public void sendMessage(final MessageCode code, final String text)
        throws InterruptedException
    {
        assert code != null;
        assert text != null;
        final Message message = toMessage(code, text);

        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                _out.putMessage(message);
            }

            @Override
            public String toString() {
                return String.format("sendMessage(%s, %s)", code, text);
            }
        };
        appendJob(j);
    }

    Collection<Pair<Boolean, FileInfo>>
    toListingPair(Collection<FileInfo> files)
    {
        Collection<Pair<Boolean, FileInfo>> listing =
                new ArrayList<>(files.size());
        for (FileInfo f: files) {
            listing.add(new Pair<>(true, f));
        }
        return listing;
    }

    public void generateSegment(final Filelist.Segment segment)
        throws InterruptedException
    {
        assert segment != null;

        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                if (_isListOnly) {
                    Collection<FileInfo> c;
                    if (_fileSelection != FileSelection.RECURSE) {
                        c = segment.files();
                    } else if (segment.directory() == null) {
                        c = listInitialSegmentRecursive(segment);
                    } else {
                        c = listSegmentRecursive(segment);
                    }
                    _listing.addAll(toListingPair(c));
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
        assert segment != null;
        assert fileInfo != null;
        assert fileInfo.isTransferrable();

        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                try {
                    boolean isTransfer =
                            itemizeFile(fileIndex, fileInfo,
                                        Checksum.MAX_DIGEST_LENGTH);
                    if (!isTransfer) {
                        segment.remove(fileIndex);
                        removeAllFinishedSegmentsAndNotifySender();
                    }
                } catch (IOException e) {
                    String msg = String.format("failed to generate file meta " +
                                               "data for %s (index %d): %s",
                                               fileInfo.pathOrNull(),
                                               fileIndex,
                                               e.getMessage());
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(msg);
                    }
                    _out.putMessage(toMessage(MessageCode.ERROR_XFER,
                                              msg + '\n'));
                    _returnStatus++;
                }
            }

            @Override
            public String toString()
            {
                return String.format("generateFile (%s, %d, %s)",
                                     segment, fileIndex, fileInfo.pathOrNull());
            }
        };
        appendJob(j);
    }

    public void sendSegmentDone() throws InterruptedException
    {
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                _out.encodeIndex(Filelist.DONE);
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
        assert dir != null;
        assert dir.isTransferrable();
        RsyncFileAttributes attrs =
                RsyncFileAttributes.statOrNull(dir.pathOrNull());
        if (attrs == null) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("(Generator) creating directory " + dir.pathOrNull());
            }
            Files.createDirectories(dir.pathOrNull());
        }
        deferUpdateAttrsIfDiffer(dir.pathOrNull(), attrs, dir.attrs());
    }

    private int itemizeSegment(Filelist.Segment segment) throws ChannelException
    {
        int numErrors = 0;
        List<Integer> toRemove = new LinkedList<>();

        for (Map.Entry<Integer, FileInfo> entry : segment.entrySet()) {
            final int index = entry.getKey();
            final FileInfo f = entry.getValue();
            boolean isTransfer = false;
            try {
                if (f.isTransferrable()) {
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine(String.format(
                                "(Generator) generating %s, index %d",
                                f.pathOrNull(), index));
                    }
                    if (f.attrs().isRegularFile()) {
                        isTransfer = itemizeFile(index, f,
                                                 Checksum.MIN_DIGEST_LENGTH);
                    } else if (f.attrs().isDirectory()) {
                        if (_fileSelection != FileSelection.RECURSE) {
                            itemizeDirectory(index, f);
                        }
                    } else if (f instanceof DeviceInfo &&
                               _isPreserveDevices &&
                               (f.attrs().isBlockDevice() ||
                                f.attrs().isCharacterDevice())) {
                        itemizeDevice(index, (DeviceInfo) f);
                    } else if (f instanceof DeviceInfo &&
                               _isPreserveSpecials &&
                               (f.attrs().isFifo() ||
                                f.attrs().isSocket())) {
                        itemizeDevice(index, (DeviceInfo) f);
                    } else if (_isPreserveLinks && f instanceof SymlinkInfo) {
                        itemizeSymlink(index, (SymlinkInfo) f);
                    } else {
                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("(Generator) Skipping " + f.pathOrNull());
                        }
                    }
                }
            } catch (IOException e) {
                if (f.attrs().isDirectory()) {
                    // we cannot remove the corresponding segment since we may
                    // not have received it yet
                    prune(index);
                }
                String msg = String.format(
                        "failed to generate %s %s (index %d): %s",
                        FileOps.fileTypeToString(f.attrs().mode()),
                        f.pathOrNull(),
                        index,
                        e.getMessage());
                if (_log.isLoggable(Level.WARNING)) {
                    _log.warning(msg);
                }
                _out.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                numErrors++;
            }
            if (!isTransfer) {
                toRemove.add(index);
            }
        }
        segment.removeAll(toRemove);
        return numErrors;
    }

    private Collection<FileInfo>
    listInitialSegmentRecursive(Filelist.Segment segment)
    {
        assert _fileSelection == FileSelection.RECURSE;
        assert segment.directory() == null;
        boolean listFirstDotDir = true;
        Collection<FileInfo> res = new ArrayList<>(segment.files().size());
        for (FileInfo f : segment.files()) {
            if (!f.attrs().isDirectory()) {
                res.add(f);
            } else if (listFirstDotDir) {
                if (f.isDotDir()) {
                    res.add(f);
                }
                listFirstDotDir = false;
            }
        }
        return res;
    }

    private Collection<FileInfo> listSegmentRecursive(Filelist.Segment segment)
    {
        assert _fileSelection == FileSelection.RECURSE;
        assert segment.directory() != null;
        Collection<FileInfo> res = new ArrayList<>(segment.files().size());
        res.add(segment.directory());
        for (FileInfo f : segment.files()) {
            if (!f.attrs().isDirectory()) {
                res.add(f);
            }
        }
        return res;
    }

    private void sendChecksumForSegment(Filelist.Segment segment)
        throws ChannelException
    {
        assert !_isListOnly;
        assert segment != null;

        final int dirIndex = segment.directoryIndex();
        FileInfo dir = segment.directory();
        if (dir != null && (isPruned(dirIndex) || !dir.isTransferrable())) {
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

        if (_isDelete && _isDeletionsEnabled) {
            try {
                unlinkFilesInDirNotAtSender(dir.pathOrNull(), segment.files());
            } catch (IOException e) {
                if (Files.exists(dir.pathOrNull(), LinkOption.NOFOLLOW_LINKS)) {
                    String msg = String.format("failed to delete %s and all " +
                                               "its files: %s",
                                               dir, e);
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(msg);
                    }
                    _out.putMessage(toMessage(MessageCode.ERROR_XFER,
                                              msg + '\n'));
                    _returnStatus++;
                }
            }
        }
        try {
            if (dir.attrs().isDirectory()) {
                mkdir(dir);
            }
            if (!isInitialFileList) {
                itemizeDirectory(dirIndex, dir);
            }
            _returnStatus += itemizeSegment(segment);
        } catch (IOException e) {
            String msg = String.format(
                    "failed to generate files below dir %s (index %d): %s",
                    dir.pathOrNull(), dirIndex, e.getMessage());
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning(msg);
            }
            _out.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
            segment.removeAll();
            _returnStatus++;
        }
    }

    private static boolean isDataModified(RsyncFileAttributes curAttrsOrNull,
                                          RsyncFileAttributes newAttrs)
    {
        assert newAttrs != null;
        return curAttrsOrNull == null ||
               curAttrsOrNull.size() != newAttrs.size() ||
               curAttrsOrNull.lastModifiedTime() != newAttrs.lastModifiedTime();
    }

    private void sendChecksumHeader(Checksum.Header header)
        throws ChannelException
    {
        Connection.sendChecksumHeader(_out, header);
    }

    private static int getBlockLengthFor(long fileSize)
    {
        assert fileSize >= 0;
        if (fileSize == 0) {
            return 0;
        }
        int blockLength = pow2SquareRoot(fileSize);
        assert fileSize / blockLength <= Integer.MAX_VALUE;
        return Math.max(MIN_BLOCK_SIZE, blockLength);
    }

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
        assert fileInfo != null;
        assert fileInfo.isTransferrable();
        assert curAttrs != null;

        long currentSize = curAttrs.size();
        int blockLength = getBlockLengthFor(currentSize);
        int windowLength = blockLength;
        int digestLength = currentSize > 0
                           ? Math.max(minDigestLength,
                                      getDigestLength(currentSize, blockLength))
                           : 0;
        // new FileView() throws FileViewOpenFailed
        try (FileView fv = new FileView(fileInfo.pathOrNull(),
                                        currentSize,
                                        blockLength,
                                        windowLength)) {

            // throws ChunkCountOverflow
            Checksum.Header header = new Checksum.Header(blockLength,
                                                         digestLength,
                                                         currentSize);
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
                _out.putInt(rolling);
                md.update(fv.array(), fv.startOffset(), fv.windowLength());
                md.update(_checksumSeed);
                _out.put(md.digest(), 0, digestLength);
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
        } catch (FileViewReadError e) {
            // occurs at FileView.close() - if there were any I/O errors during
            // file read
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning("(Generator) Warning got I/O errors during " +
                             "checksum generation. Errors ignored and data " +
                             "filled with zeroes): " + e.getMessage());
            }
        }
    }

    private void updateAttrsIfDiffer(Path path,
                                     RsyncFileAttributes curAttrsOrNull,
                                     RsyncFileAttributes newAttrs)
        throws IOException
    {
        assert path != null;
        assert newAttrs != null;

        if (_isPreservePermissions &&
            (curAttrsOrNull == null ||
             curAttrsOrNull.mode() != newAttrs.mode()))
        {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                        "(Generator) %s: updating mode %o -> %o",
                        path,
                        curAttrsOrNull == null ? 0 : curAttrsOrNull.mode(),
                        newAttrs.mode()));
            }
            FileOps.setFileMode(path, newAttrs.mode(),
                                LinkOption.NOFOLLOW_LINKS);
        }
        if (_isPreserveTimes &&
            (curAttrsOrNull == null ||
             curAttrsOrNull.lastModifiedTime() != newAttrs.lastModifiedTime()))
        {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                    "(Generator) %s: updating mtime %s -> %s",
                    path,
                    curAttrsOrNull == null
                            ? _dateFormat.format(
                                    new Date(
                                       FileTime.from(
                                            0L,
                                            TimeUnit.SECONDS).toMillis()))
                            : _dateFormat.format(
                                    new Date(
                                        FileTime.from(
                                            curAttrsOrNull.lastModifiedTime(),
                                            TimeUnit.SECONDS).toMillis())),
                    _dateFormat.format(
                            new Date(FileTime.from(
                                            newAttrs.lastModifiedTime(),
                                            TimeUnit.SECONDS).toMillis()))));

            }
            FileOps.setLastModifiedTime(path, newAttrs.lastModifiedTime(),
                                        LinkOption.NOFOLLOW_LINKS);
        }
        // NOTE: keep this one last in the method, in case we fail due to
        //       insufficient permissions (the other ones are more likely to
        //       succeed).
        // NOTE: we cannot detect if we have the capabilities to change
        //       ownership (knowing if UID 0 is not sufficient)
        if (_isPreserveUser) {
            if (!_isNumericIds && !newAttrs.user().name().isEmpty() &&
                (curAttrsOrNull == null ||
                 !curAttrsOrNull.user().name().equals(newAttrs.user().name())))
            {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                        "(Generator) %s: updating ownership %s -> %s",
                        path,
                        curAttrsOrNull == null ? "" : curAttrsOrNull.user(),
                        newAttrs.user()));
                }
                // NOTE: side effect of chown in Linux is that set user/group id
                // bit might be cleared.
                FileOps.setOwner(path, newAttrs.user(),
                                 LinkOption.NOFOLLOW_LINKS);
            } else if ((_isNumericIds || newAttrs.user().name().isEmpty()) &&
                       (curAttrsOrNull == null ||
                        curAttrsOrNull.user().id() != newAttrs.user().id())) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                        "(Generator) %s: updating ownership %s -> %s",
                        path,
                        curAttrsOrNull == null ? "" : curAttrsOrNull.user().id(),
                        newAttrs.user().id()));
                }
                // NOTE: side effect of chown in Linux is that set user/group id
                // bit might be cleared.
                FileOps.setUserId(path, newAttrs.user().id(),
                                  LinkOption.NOFOLLOW_LINKS);
            }
        }

        if (_isPreserveGroup) {
            if (!_isNumericIds && !newAttrs.group().name().isEmpty() &&
                (curAttrsOrNull == null ||
                 !curAttrsOrNull.group().name().equals(newAttrs.group().name()))) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                            "(Generator) %s: updating group %s -> %s",
                            path,
                            curAttrsOrNull == null
                                ? "" : curAttrsOrNull.group(),
                            newAttrs.group()));
                }
                FileOps.setGroup(path, newAttrs.group(),
                                 LinkOption.NOFOLLOW_LINKS);
            } else if ((_isNumericIds || newAttrs.group().name().isEmpty()) &&
                       (curAttrsOrNull == null ||
                        curAttrsOrNull.group().id() != newAttrs.group().id())) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                            "(Generator) %s: updating gid %s -> %d",
                            path,
                            curAttrsOrNull == null
                                ? "" : curAttrsOrNull.group().id(),
                            newAttrs.group().id()));
                }
                FileOps.setGroupId(path, newAttrs.group().id(),
                                   LinkOption.NOFOLLOW_LINKS);
            }
        }
    }

    private void deferUpdateAttrsIfDiffer(
                                    final Path path,
                                    final RsyncFileAttributes curAttrsOrNull,
                                    final RsyncFileAttributes newAttrs)
    {
        assert path != null;
        assert newAttrs != null;

        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                try {
                    updateAttrsIfDiffer(path, curAttrsOrNull, newAttrs);
                } catch (IOException e) {
                    String msg = String.format(
                            "received I/O error while applying " +
                            "attributes on %s: %s", path, e.getMessage());
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(msg);
                    }
                    _out.putMessage(toMessage(MessageCode.ERROR_XFER,
                                              msg + '\n'));
                    _returnStatus++;
                }
            }
        };
        _deferredJobs.addFirst(j);
    }

    private boolean itemizeFile(int index, FileInfo fileInfo, int digestLength)
        throws ChannelException, IOException
    {
        assert fileInfo != null;
        assert fileInfo.isTransferrable();

        RsyncFileAttributes curAttrsOrNull = deleteIfDifferentType(fileInfo);

        // NOTE: native opens the file first though even if its file size is
        // zero
        if (isDataModified(curAttrsOrNull, fileInfo.attrs()) || _isIgnoreTimes)
        {
            if (curAttrsOrNull == null) {
                sendItemizeInfo(index,
                                null /* curAttrsOrNull */,
                                fileInfo.attrs(),
                                Item.TRANSFER);
                sendChecksumHeader(ZERO_SUM);
            } else {
                sendItemizeAndChecksums(index, fileInfo, curAttrsOrNull,
                                        digestLength);
            }
            return true;
        }

        if (_isAlwaysItemize) {
            sendItemizeInfo(index, curAttrsOrNull, fileInfo.attrs(),
                            Item.NO_CHANGE);
        }

        try {
            updateAttrsIfDiffer(fileInfo.pathOrNull(), curAttrsOrNull,
                                fileInfo.attrs());
        } catch (IOException e) {
            String msg = String.format(
                    "received an I/O error while applying attributes on %s: %s",
                    fileInfo.pathOrNull(), e.getMessage());
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning(msg);
            }
            _out.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
            _returnStatus++;
        }
        return false;
    }

    private char itemizeFlags(RsyncFileAttributes curAttrsOrNull,
                              RsyncFileAttributes newAttrs)
    {
        assert newAttrs != null;

        if (curAttrsOrNull == null) {
            return Item.IS_NEW;
        }
        char iFlags = Item.NO_CHANGE;
        if (_isPreservePermissions &&
            curAttrsOrNull.mode() != newAttrs.mode())
        {
            iFlags |= Item.REPORT_PERMS;
        }
        if (_isPreserveTimes &&
            curAttrsOrNull.lastModifiedTime() != newAttrs.lastModifiedTime())
        {
            iFlags |= Item.REPORT_TIME;
        }
        if (_isPreserveUser &&
            !curAttrsOrNull.user().equals(newAttrs.user()))
        {
            iFlags |= Item.REPORT_OWNER;
        }
        if (_isPreserveGroup &&
            !curAttrsOrNull.group().equals(newAttrs.group()))
        {
            iFlags |= Item.REPORT_GROUP;
        }
        if (curAttrsOrNull.isRegularFile() &&
            curAttrsOrNull.size() != newAttrs.size())
        {
            iFlags |= Item.REPORT_SIZE;
        }
        return iFlags;
    }

    private void sendItemizeInfo(int index,
                                 RsyncFileAttributes curAttrsOrNull,
                                 RsyncFileAttributes newAttrs,
                                 char iMask)
        throws ChannelException
    {
        assert newAttrs != null;
        char iFlags = (char) (iMask | itemizeFlags(curAttrsOrNull, newAttrs));
        if (_log.isLoggable(Level.FINE)) {
            _log.fine("(Generator) sending itemizeFlags=" + (int) iFlags);
        }
        _out.encodeIndex(index);
        _out.putChar(iFlags);
    }

    private void itemizeDirectory(int index, FileInfo fileInfo)
        throws ChannelException,IOException
    {
        assert fileInfo != null;
        assert fileInfo.isTransferrable();

        RsyncFileAttributes curAttrsOrNull = deleteIfDifferentType(fileInfo);
        if (curAttrsOrNull == null) {
            sendItemizeInfo(index, null /* curAttrsOrNull */, fileInfo.attrs(),
                            Item.LOCAL_CHANGE);
            mkdir(fileInfo);   // throws IOException
        } else {
            if (_isAlwaysItemize) {
                sendItemizeInfo(index, curAttrsOrNull, fileInfo.attrs(),
                                Item.NO_CHANGE);
            }
            if (!curAttrsOrNull.equals(fileInfo.attrs())) {
                deferUpdateAttrsIfDiffer(fileInfo.pathOrNull(),
                                         curAttrsOrNull, fileInfo.attrs());
            }
        }
    }

    RsyncFileAttributes deleteIfDifferentType(FileInfo fileInfo)
            throws IOException
    {
        // null if file does not exist; throws IOException on any other error
        RsyncFileAttributes curAttrsOrNull =
                RsyncFileAttributes.statIfExists(fileInfo.pathOrNull());
        if (curAttrsOrNull != null &&
            curAttrsOrNull.fileType() != fileInfo.attrs().fileType()) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                        "deleting %s %s (expecting a %s)",
                        FileOps.fileType(curAttrsOrNull.mode()),
                        fileInfo.pathOrNull(),
                        FileOps.fileType(fileInfo.attrs().mode())));
            }
            FileOps.unlink(fileInfo.pathOrNull());
            return null;
        } else {
            return curAttrsOrNull;
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
        for (Iterator<Filelist.Segment> it = _generated.iterator(); it.hasNext(); )
        {
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
            // identity comparison
            if (deleted != segment) {
                throw new IllegalStateException(String.format("%s != %s",
                                                              deleted,
                                                              segment));
            }
            // NOTE: remove before notifying peer
            it.remove();
            _out.encodeIndex(Filelist.DONE);
        }
    }

    public synchronized long numBytesWritten()
    {
        return _out.numBytesWritten();
    }

    @Override
    public Iterator<FileInfo> iterator()
    {
        return new Iterator<FileInfo>() {
            private Pair<Boolean, FileInfo> _next;

            @Override
            public boolean hasNext()
            {
                try {
                    _next = _listing.take();
                    return _next.first();
                } catch (InterruptedException e) {
                    throw new RuntimeInterruptException(e);
                }
            }

            @Override
            public FileInfo next()
            {
                return _next.second();
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public void prune(int index)
    {
        _pruned.set(index);
    }

    public boolean isPruned(int index)
    {
        return _pruned.get(index);
    }

    public boolean isPreserveDevices()
    {
        return _isPreserveDevices;
    }

    public boolean isPreserveLinks()
    {
        return _isPreserveLinks;
    }

    public boolean isPreserveSpecials()
    {
        return _isPreserveSpecials;
    }

    private void itemizeDevice(int index, DeviceInfo dev) throws IOException
    {
        throw new IOException("unable to generate device file - operation " +
                              "not supported");
    }

    private void itemizeSymlink(int index, SymlinkInfo linkInfo)
            throws IOException, ChannelException
    {
        try {
            RsyncFileAttributes curAttrsOrNull = deleteIfDifferentType(linkInfo);
            if (curAttrsOrNull != null) {
                Path curTarget = Files.readSymbolicLink(linkInfo.pathOrNull());
                if (curTarget.equals(linkInfo.targetPath())) {
                    if (_isAlwaysItemize) {
                        sendItemizeInfo(index, curAttrsOrNull, linkInfo.attrs(),
                                        Item.NO_CHANGE);
                    }
                    return;
                }
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                            "deleting symlink %s -> %s",
                            linkInfo.pathOrNull(), curTarget));
                }
                Files.deleteIfExists(linkInfo.pathOrNull());
            }
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("creating symlink %s -> %s",
                                        linkInfo.pathOrNull(),
                                        linkInfo.targetPath()));
            }
            Files.createSymbolicLink(linkInfo.pathOrNull(),
                                     linkInfo.targetPath());

            sendItemizeInfo(index,
                            null /* curAttrsOrNull */,
                            linkInfo.attrs(),
                            (char) (Item.LOCAL_CHANGE | Item.REPORT_CHANGE));
        } catch (UnsupportedOperationException e) {
            throw new IOException(e);
        }
    }

    private void unlinkFilesInDirNotAtSender(Path dir,
                                             Collection<FileInfo> files)
            throws IOException, ChannelException
    {
        assert _isDelete && _isDeletionsEnabled;
        Set<Path> senderPaths = new HashSet<>(files.size());
        for (FileInfo f : files) {
            if (f.pathOrNull() != null) {
                senderPaths.add(f.pathOrNull());
            }
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (!senderPaths.contains(entry)) {
                    try {
                        if (_log.isLoggable(Level.INFO)) {
                            _log.info("deleting extraneous " + entry);
                        }
                        FileOps.unlink(entry);
                    } catch (IOException e) {
                        String msg = String.format("failed to delete %s: %s",
                                                   entry, e);
                        if (_log.isLoggable(Level.WARNING)) {
                            _log.warning(msg);
                        }
                        _out.putMessage(toMessage(MessageCode.ERROR_XFER,
                                                  msg + '\n'));
                        _returnStatus++;

                    }
                }
            }
        }
    }

    public void disableDelete()
    {
        if (_isDelete && _isDeletionsEnabled) {
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning("--delete disabled due to receiving error " +
                             "notification from peer sender");
            }
            _isDeletionsEnabled = false;
        }
    }

    public void processDeferredJobs() throws InterruptedException
    {
        Job job = new Job() {
            @Override
            public void process() throws ChannelException {
                for (Job j : _deferredJobs) {
                    j.process();
                }
            }
            @Override
            public String toString() {
                return "processDeferredJobs()";
            }
        };
        appendJob(job);
    }
}
