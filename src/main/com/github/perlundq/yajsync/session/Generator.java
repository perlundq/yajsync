/*
 * Receiver -> Sender communication, generation of file meta data +
 * checksum info to peer Sender
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
package com.github.perlundq.yajsync.session;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
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

public class Generator
{
    private interface Job {
        void process() throws ChannelException;
    }

    private static final Logger _log =
        Logger.getLogger(Generator.class.getName());
    private static final int OUTPUT_CHANNEL_BUF_SIZE = 8 * 1024;
    private static final Checksum.Header ZERO_SUM;
    private static final int MIN_BLOCK_SIZE = 512;                              // TODO: make block size configurable
    private final RsyncOutChannel _senderOutChannel;
    private final byte[] _checksumSeed;

    private final LinkedBlockingQueue<Job> _jobs = new LinkedBlockingQueue<>();
    private final List<FileInfo> _filesWithWrongAttributes = new LinkedList<>();
    private final TextEncoder _characterEncoder;
    private final TextDecoder _characterDecoder;
    private final SimpleDateFormat _compatibleTimeFormatter =
        new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private final List<Filelist.Segment> _generated = new LinkedList<>();
    private boolean _isAlwaysItemize;
    private boolean _isRecursive;
    private boolean _isPreserveTimes;
    private boolean _isListOnly;
    private Filelist _fileList;  // effectively final
    private int _returnStatus ;

    static {
        try {
            ZERO_SUM = new Checksum.Header(0,0,0);
        } catch (Checksum.ChunkOverflow e) {
            throw new RuntimeException(e);
        }
    }

    public Generator(WritableByteChannel out, Charset charset,
                     byte[] checksumSeed)
    {

        _senderOutChannel = new RsyncOutChannel(out, OUTPUT_CHANNEL_BUF_SIZE);
        _checksumSeed = checksumSeed;
        _characterDecoder = TextDecoder.newStrict(charset);
        _characterEncoder = TextEncoder.newStrict(charset);
    }

    public void setIsRecursive(boolean isRecursive)
    {
        _isRecursive = isRecursive;
    }

    public boolean isRecursive()
    {
        return _isRecursive;
    }

    public void setIsListOnly(boolean isListOnly)
    {
        _isListOnly = isListOnly;
    }

    public boolean isListOnly()
    {
        return _isListOnly;
    }

    public void setIsPreserveTimes(boolean isPreserveTimes)
    {
        _isPreserveTimes = isPreserveTimes;
    }

    public boolean isPreserveTimes()
    {
        return _isPreserveTimes;
    }

    public void setIsAlwaysItemize(boolean isAlwaysItemize)
    {
        _isAlwaysItemize = isAlwaysItemize;
    }

    public boolean isAlwaysItemize()
    {
        return _isAlwaysItemize;
    }

    /**
     * @throws IllegalStateException if fileList is already set
     */
    public void setFileList(Filelist fileList)
    {
        if (_fileList != null) {
            throw new IllegalStateException("file list may only be set once");
        }
        _fileList = fileList;
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
        while (!Thread.currentThread().isInterrupted()) {
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
                _senderOutChannel.flush();
            }
        }
    }

    public boolean generate() throws ChannelException
    {
        try {
            //processJobQueueImmediate();
            processJobQueueBatched();
        } catch (InterruptedException | RuntimeInterruptException e) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("(Generator) thread interrupted");
            }
            return false;
        }
        return _returnStatus == 0;
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
            public void process() throws ChannelException {
                _senderOutChannel.flush();
                applyCorrectAttributes();
                Thread.currentThread().interrupt();
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
        final Message message = new Message(MessageCode.ERROR_XFER, payload);

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
                    listSegment(segment);
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
            gotDirtyAttribute(dir);
        } else if (attrs.lastModifiedTime() != dir.attrs().lastModifiedTime()) { // FIXME: generalize generator dirty attribute testing
            gotDirtyAttribute(dir);
        }
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
                    } else if (!_isRecursive && f.attrs().isDirectory()) {
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

    private void listSegment(Filelist.Segment segment)
    {
        FileInfo dir = segment.directory();
        boolean listFirstDir = dir == null; // dir is only null for initial file list
        if (dir != null) {
            System.out.println(listFileInfo(dir)); // FIXME: don't hardcode System.out?
        }
        for (FileInfo f : segment.files()) {
            if (!_isRecursive ||
                !f.attrs().isDirectory() || listFirstDir) {
                System.out.println(listFileInfo(f));
                listFirstDir = false;
            }
        }
        segment.removeAll();
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

    private static boolean isTimeEquals(RsyncFileAttributes left,
                                        RsyncFileAttributes right)
    {
        return left.lastModifiedTime() == right.lastModifiedTime();
    }

    private void sendChecksumHeader(Checksum.Header header)
        throws ChannelException
    {
        Connection.sendChecksumHeader(_senderOutChannel, header);
    }

    private int getCompatibleBlockLengthFor(long fileSize)
    {
        final int BLOCK_SIZE = 700;
        final int MAX_BLOCK_SIZE = 1 << 17;

        if (fileSize <= BLOCK_SIZE * BLOCK_SIZE) {
            return BLOCK_SIZE;
        }

        int c = 1;
        for (long l = fileSize; (l >>>= 2) != 0; c <<= 1) {
            //
        }

        if (c < 0 || c >= MAX_BLOCK_SIZE) {
            return MAX_BLOCK_SIZE;
        }

        int blength = 0;
        do {
            blength |= c;
            if (fileSize < blength * blength)
                blength &= ~c;
            c >>= 1;
        } while (c >= 8);
        return Math.max(blength, BLOCK_SIZE);
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
                                         RsyncFileAttributes existingAttrs,
                                         int minDigestLength)
        throws ChannelException
    {
        long currentSize = existingAttrs.size();
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

            sendItemizeInfo(index, fileInfo, existingAttrs, Item.TRANSFER);
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
            sendItemizeInfo(index, fileInfo, null, Item.TRANSFER);
            sendChecksumHeader(ZERO_SUM);
        } catch (FileViewReadError e) { // from FileView.close() if there were any I/O errors during file read
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning("(Generator) Warning got I/O errors during " +
                             "checksum generation. Errors ignored and data " +
                             "filled with zeroes): " + e.getMessage());
            }
        }
    }

    private boolean itemizeFile(int index,
                                FileInfo fileInfo,
                                RsyncFileAttributes existingAttrs,
                                int digestLength)
        throws ChannelException
    {
        // NOTE: native opens the file first though even if its file size is zero
        if (isDataModified(fileInfo.attrs(), existingAttrs)) {
            if (existingAttrs == null) {
                sendItemizeInfo(index, fileInfo, existingAttrs, Item.TRANSFER);
                sendChecksumHeader(ZERO_SUM);
            } else {
                sendItemizeAndChecksums(index, fileInfo, existingAttrs,
                                        digestLength);
            }
            return true;
        }

        if (_isAlwaysItemize) {
            sendItemizeInfo(index, fileInfo, existingAttrs, Item.NO_CHANGE);
        }
        // FIXME: only set the attributes that we are interested in preserving
        // FIXME: compare only settable attributes
        if (!fileInfo.attrs().isSettableAttributesEquals(existingAttrs)) {
            try {
                FileOps.setFileAttributes(fileInfo.path(), fileInfo.attrs());
            } catch (IOException e) {
                if (_log.isLoggable(Level.WARNING)) {
                    _log.warning(String.format(
                        "(Generator) received I/O error while applying " +
                        "attributes on %s: %s",
                        fileInfo.path(), e.getMessage()));
                }
                _returnStatus++;
            }
        }
        return false;
    }

    private char itemizeFlags(FileInfo fileInfo,
                              RsyncFileAttributes existingAttrs)
    {
        if (existingAttrs == null) {
            return Item.IS_NEW;
        }

        char iFlags = Item.NO_CHANGE;

        if (_isPreserveTimes &&
            !isTimeEquals(fileInfo.attrs(), existingAttrs)) {
            iFlags |= Item.REPORT_TIME;
        }

        if (fileInfo.attrs().isRegularFile() &&
            fileInfo.attrs().size() != existingAttrs.size()) {
            iFlags |= Item.REPORT_SIZE;
        }

        return iFlags;
    }

    private void sendItemizeInfo(int index,
                                 FileInfo fileInfo,
                                 RsyncFileAttributes existingAttrs,
                                 char iMask)
        throws ChannelException
    {
        char iFlags = (char) (iMask | itemizeFlags(fileInfo, existingAttrs));
        if (_log.isLoggable(Level.FINE)) {
            _log.fine("(Generator) sending itemizeFlags=" + (int) iFlags);
        }
        _senderOutChannel.encodeIndex(index);
        _senderOutChannel.putChar(iFlags);
    }

    private void itemizeDirectory(int index,
                                  FileInfo fileInfo,
                                  RsyncFileAttributes existingAttrs)
        throws ChannelException,IOException
    {
        if (existingAttrs == null) {
            sendItemizeInfo(index, fileInfo, existingAttrs, Item.LOCAL_CHANGE);
            mkdir(fileInfo);   // throws IOException
        } else {
            if (_isAlwaysItemize) {
                sendItemizeInfo(index, fileInfo, existingAttrs, Item.NO_CHANGE);
            }

            // FIXME:
//            if (fileInfo.attrs().mode() != existingAttrs.mode()) { // update if we add support for modifying other stuff then the file mode
            if (fileInfo.attrs().lastModifiedTime() != existingAttrs.lastModifiedTime()) { // update if we add support for modifying other stuff then the file mode
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                        "(Generator) %s != %s, attribute update postponed",
                        fileInfo.attrs(), existingAttrs));
                }
                gotDirtyAttribute(fileInfo);
            }
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

    private void gotDirtyAttribute(FileInfo fileInfo)
    {
        if (_isPreserveTimes) {
            _filesWithWrongAttributes.add(fileInfo);
        }
    }

    private void applyCorrectAttributes()
    {
        Collections.reverse(_filesWithWrongAttributes);
        for (FileInfo f : _filesWithWrongAttributes) {
            try {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("(Generator) applying correct attributes on " +
                              f);
                }
                FileOps.setFileAttributes(f.path(), f.attrs());
            } catch (IOException e) {
                if (_log.isLoggable(Level.WARNING)) {
                    _log.warning(String.format(
                        "(Generator) failed to set attributes on %s: %s",
                        f.path(), e.getMessage()));
                }
                _returnStatus++;
            }
        }
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
