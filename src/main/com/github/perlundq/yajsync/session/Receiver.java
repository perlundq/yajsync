/*
 * Processing of incoming file lists and file data from Sender
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
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.channels.ChannelEOFException;
import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.channels.Message;
import com.github.perlundq.yajsync.channels.MessageCode;
import com.github.perlundq.yajsync.channels.MessageHandler;
import com.github.perlundq.yajsync.channels.RsyncInChannel;
import com.github.perlundq.yajsync.filelist.ConcurrentFilelist;
import com.github.perlundq.yajsync.filelist.FileInfo;
import com.github.perlundq.yajsync.filelist.Filelist;
import com.github.perlundq.yajsync.filelist.RsyncFileAttributes;
import com.github.perlundq.yajsync.io.CustomFileSystem;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.text.TextConversionException;
import com.github.perlundq.yajsync.text.TextDecoder;
import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.FileOps;
import com.github.perlundq.yajsync.util.MD5;
import com.github.perlundq.yajsync.util.PathOps;
import com.github.perlundq.yajsync.util.RuntimeInterruptException;
import com.github.perlundq.yajsync.util.Util;

public class Receiver implements RsyncTask,MessageHandler
{
    @SuppressWarnings("serial")
    private class PathResolverException extends Exception {
        public PathResolverException(String msg) {
            super(msg);
        }
    }

    private static class FileInfoStub {
        private final String _pathName;
        private final byte[] _pathNameBytes;
        private final RsyncFileAttributes _attrs;

        private FileInfoStub(String pathName, byte[] pathNameBytes,
                             RsyncFileAttributes attrs) {
            _pathName = pathName;
            _pathNameBytes = pathNameBytes;
            _attrs = attrs;
        }

        @Override
        public String toString()
        {
            return String.format("%s %s", _pathName, _attrs);
        }
    }

    private interface PathResolver
    {
        /**
         * @throws InvalidPathException
         * @throws RsyncSecurityException
         */
        Path relativePathOf(String pathName);

        /**
         * @throws RsyncSecurityException
         */
        Path fullPathOf(Path relativePath);
    }

    private static final Logger _log =
        Logger.getLogger(Receiver.class.getName());

    private static final int INPUT_CHANNEL_BUF_SIZE = 8 * 1024;
    private final FileInfoCache _fileInfoCache = new FileInfoCache();
    private final Generator _generator;
    private final RsyncInChannel _senderInChannel;
    private final Statistics _stats = new Statistics();
    private final TextDecoder _characterDecoder;
    private final String _targetPathName;
    private boolean _isSendFilterRules;
    private boolean _isReceiveStatistics;
    private boolean _isExitEarlyIfEmptyList;
    private boolean _isRecursive;
    private boolean _isListOnly;
    private boolean _isPreservePermissions;
    private boolean _isPreserveTimes;
    private boolean _isDeferredWrite;
    private boolean _isInterruptible = true;
    private boolean _isExitAfterEOF;
    private int _ioError;
    private PathResolver _pathResolver;

    public Receiver(Generator generator,
                    ReadableByteChannel in,
                    Charset charset,
                    String targetPathName)
    {
        _senderInChannel = new RsyncInChannel(in,
                                              this,
                                              INPUT_CHANNEL_BUF_SIZE);
        _characterDecoder = TextDecoder.newStrict(charset);
        _generator = generator;
        _targetPathName = targetPathName;
    }

    public static Receiver newServerInstance(Generator generator,
                                             ReadableByteChannel in,
                                             Charset charset,
                                             String targetPathName)
    {
        return new Receiver(generator, in, charset, targetPathName).
            setIsSendFilterRules(false).
            setIsReceiveStatistics(false).
            setIsExitEarlyIfEmptyList(false).
            setIsListOnly(false);
    }

    public static Receiver newClientInstance(Generator generator,
                                             ReadableByteChannel in,
                                             Charset charset,
                                             String targetPathName)
    {
        return new Receiver(generator, in, charset, targetPathName).
            setIsSendFilterRules(true).
            setIsReceiveStatistics(true).
            setIsExitEarlyIfEmptyList(true).
            setIsExitAfterEOF(true);
    }

    public Receiver setIsRecursive(boolean isRecursive)
    {
        _isRecursive = isRecursive;
        return this;
    }

    public Receiver setIsListOnly(boolean isListOnly)
    {
        _isListOnly = isListOnly;
        return this;
    }

    public Receiver setIsPreservePermissions(boolean isPreservePermissions)
    {
        _isPreservePermissions = isPreservePermissions;
        return this;
    }

    public Receiver setIsPreserveTimes(boolean isPreserveTimes)
    {
        _isPreserveTimes = isPreserveTimes;
        return this;
    }

    public Receiver setIsDeferredWrite(boolean isDeferredWrite)
    {
        _isDeferredWrite = isDeferredWrite;
        return this;
    }

    public Receiver setIsExitAfterEOF(boolean isExitAfterEOF)
    {
        _isExitAfterEOF = isExitAfterEOF;
        return this;
    }

    public Receiver setIsInterruptible(boolean isInterruptible)
    {
        _isInterruptible = isInterruptible;
        return this;
    }

    public Receiver setIsSendFilterRules(boolean isSendFilterRules)
    {
        _isSendFilterRules = isSendFilterRules;
        return this;
    }

    public Receiver setIsReceiveStatistics(boolean isReceiveStatistics)
    {
        _isReceiveStatistics = isReceiveStatistics;
        return this;
    }

    public Receiver setIsExitEarlyIfEmptyList(boolean isExitEarlyIfEmptyList)
    {
        _isExitEarlyIfEmptyList = isExitEarlyIfEmptyList;
        return this;
    }

    @Override
    public boolean isInterruptible()
    {
        return _isInterruptible;
    }

    @Override
    public void closeChannel() throws ChannelException
    {
        _senderInChannel.close();
    }

    @Override
    public Boolean call() throws RsyncException, InterruptedException
    {
        try {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("Receiver.receive(targetPathName=%s, " +
                                        "isDeferredWrite=%s," +
                                        " isListOnly=%s, isPreserveTimes=%s, " +
                                        "isRecursive=%s, sendFilterRules=%s, " +
                                        "receiveStatistics=%s, " +
                                        "exitEarlyIfEmptyList=%s",
                                        _targetPathName, _isDeferredWrite,
                                        _isListOnly, _isPreserveTimes,
                                        _isRecursive, _isSendFilterRules,
                                        _isReceiveStatistics,
                                        _isExitEarlyIfEmptyList));
            }
            if (_isSendFilterRules) {
                sendEmptyFilterRules();
            }

            List<FileInfoStub> stubs = new LinkedList<>();
            _ioError |= receiveFileMetaDataInto(stubs);
            if (stubs.size() == 0 && _isExitEarlyIfEmptyList) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("empty file list - exiting early");
                }
                // NOTE: we never _receive_ any statistics if initial file list is empty
                if (_isExitAfterEOF) {
                    readAllMessagesUntilEOF();
                }
                return _ioError == 0;
            }

            Path targetPath = PathOps.get(_targetPathName);                     // throws InvalidPathException
            _pathResolver = getPathResolver(targetPath, stubs);                 // throws PathResolverException
            Filelist.SegmentBuilder builder = new Filelist.SegmentBuilder(null);
            _ioError |= extractFileMetadata(stubs, builder);

            Filelist fileList = new ConcurrentFilelist(_isRecursive);           // FIXME: move out
            _generator.setFileList(fileList);                                   // FIXME: move out
            Filelist.Segment segment = fileList.newSegment(builder);
            _generator.generateSegment(segment);
            receiveFiles(fileList, segment);
            _stats.setNumFiles(fileList.numFiles());
            if (_isReceiveStatistics) {
                receiveStatistics();
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                        "(local) Total file size: %d bytes, Total bytes sent:" +
                        " %d, Total bytes received: %d",
                        fileList.totalFileSize(),
                        _generator.numBytesWritten(),
                        _senderInChannel.numBytesRead()));
                }
            }

            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("Receiver returned %d errors",
                                        _ioError));
            }
            if (_isExitAfterEOF) {
                readAllMessagesUntilEOF();
            }
            return _ioError == 0;
        } catch (RuntimeInterruptException e) {
            throw new InterruptedException();
        } catch (InvalidPathException e) { // Paths.get
            throw new RsyncException(String.format(
                "illegal target path name %s: %s", _targetPathName, e));
        } catch (PathResolverException e) { // getPathResolver
            throw new RsyncException(e);
        } finally {
            _generator.stop();
        }
    }

    /**
     * file          -> non_existing      == non_existing
     * file          -> existing_file     == existing_file
     *    *          -> existing_dir      == existing_dir/*
     *    *          -> non_existing      == non_existing/*
     *    *          -> existing_not_dir/ == fail
     *    *          -> existing_other    == fail
     * file_a file_b -> existing_not_dir  == fail
     * -r dir_a      -> existing_not_dir  == fail
     *
     * Note: one special side effect of rsync is that it treats empty dirs
     * specially (due to giving importance to the total amount of files in the
     * _two_ initial file lists), i.e.:
     *    $ mkdir empty_dir
     *    $ rsync -r empty_dir non_existing
     *    $ ls non_existing
     *    $ rsync -r empty_dir non_existing
     *    $ ls non_existing
     *    empty_dir
     * yajsync does not try to mimic this
     */
    private PathResolver getPathResolver(final Path targetPath,
                                         final List<FileInfoStub> stubs)
        throws PathResolverException
    {
        assert stubs.size() > 0;
        try {
            RsyncFileAttributes attrs =
                RsyncFileAttributes.statIfExists(targetPath);                   // throws IOException

            boolean isTargetExisting = attrs != null;
            boolean isTargetExistingDir =
                isTargetExisting && attrs.isDirectory();
            boolean isTargetExistingFile =
                isTargetExisting && attrs.isRegularFile();
            boolean isSourceSingleFile =
                stubs.size() == 1 && stubs.get(0)._attrs.isRegularFile();
            boolean isTargetNonExistingFile =
                !isTargetExisting && !targetPath.endsWith(PathOps.DOT_DIR);

            if (isSourceSingleFile && isTargetNonExistingFile ||
                isSourceSingleFile && isTargetExistingFile)
            {                                                                       // -> targetPath
                return new PathResolver() {
                    @Override public Path relativePathOf(String pathName) {
                        return CustomFileSystem.getPath(stubs.get(0)._pathName);
                    }
                    @Override public Path fullPathOf(Path relativePath) {
                        return targetPath;
                    }
                };
            }
            if (isTargetExistingDir || !isTargetExisting) {                         // -> targetPath/*
                if (!isTargetExisting) {
                    Files.createDirectories(targetPath);
                }
                return new PathResolver() {
                    @Override public Path relativePathOf(String pathName) {
                        Path relativePath = CustomFileSystem.getPath(pathName);                    // throws InvalidPathException
                        if (relativePath.isAbsolute()) {
                            throw new RsyncSecurityException(relativePath +
                                " is absolute");
                        }
                        Path normalizedRelativePath =
                            PathOps.normalizeStrict(relativePath);
                        return normalizedRelativePath;
                    }
                    @Override public Path fullPathOf(Path relativePath) {
                        Path fullPath =
                            targetPath.resolve(relativePath).normalize();
                        if (!fullPath.startsWith(targetPath.normalize())) {
                            throw new RsyncSecurityException(String.format(
                                "%s is outside of receiver destination dir %s",
                                fullPath, targetPath));
                        }
                        return fullPath;
                    }
                };
            }

            if (isTargetExisting && !attrs.isDirectory() && !attrs.isRegularFile()) {
                throw new PathResolverException(String.format(
                    "refusing to overwrite existing target path %s which is " +
                    "neither a file nor a directory (%s)", targetPath, attrs));
            }
            if (isTargetExistingFile && stubs.size() >= 2) {
                throw new PathResolverException(String.format(
                    "refusing to copy source files %s into file %s " +
                    "(%s)", stubs, targetPath, attrs));
            }
            if (isTargetExistingFile && stubs.size() == 1 &&
                stubs.get(0)._attrs.isDirectory()) {
                throw new PathResolverException(String.format(
                    "refusing to recursively copy directory %s into " +
                    "non-directory %s (%s)", stubs.get(0), targetPath, attrs));
            }

            throw new AssertionError(String.format(
                "BUG: stubs=%s targetPath=%s attrs=%s",
                stubs, targetPath, attrs));

        } catch (IOException e) {
            throw new PathResolverException(String.format(
                "unable to stat %s: %s", targetPath, e));
        }
    }

    private void receiveStatistics() throws ChannelException
    {
        long totalWritten = receiveAndDecodeLong(3);
        long totalRead = receiveAndDecodeLong(3);
        long totalFileSize = receiveAndDecodeLong(3);
        long fileListBuildTime = receiveAndDecodeLong(3);
        long fileListTransferTime = receiveAndDecodeLong(3);
        _stats.setFileListBuildTime(fileListBuildTime);
        _stats.setFileListTransferTime(fileListTransferTime);
        _stats.setTotalRead(totalRead);
        _stats.setTotalFileSize(totalFileSize);
        _stats.setTotalWritten(totalWritten);
    }

    private void sendEmptyFilterRules() throws InterruptedException
    {
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(0);
        buf.flip();
        _generator.sendBytes(buf);
    }

    public Statistics statistics()
    {
        return _stats;
    }

    /**
     * @throws RsyncProtocolException if peer sends a message we cannot decode
     */
    @Override
    public void handleMessage(Message message)
    {
        switch (message.header().messageType()) {
        case IO_ERROR:
            _ioError |= message.payload().getInt();
            break;
        case NO_SEND:
            int index = message.payload().getInt();
            handleMessageNoSend(index);
            break;
        case INFO:
        case ERROR:
        case ERROR_XFER:
        case WARNING:
        case LOG:
            printMessage(message);
            break;
        default:
            throw new RuntimeException(
                "TODO: (not yet implemented) missing case statement for " +
                message);
        }
    }

    /**
     * @throws RsyncProtocolException if peer sends a message we cannot decode
     */
    private void printMessage(Message message)
    {
        assert message.isText();
        try {
            MessageCode msgType = message.header().messageType();
            if (msgType.equals(MessageCode.ERROR_XFER)) {
                _ioError |= IoError.TRANSFER;                        // this is not what native does though - it stores it in a separate variable called got_xfer_error
            }
            if (_log.isLoggable(message.logLevelOrNull())) {
                String text = _characterDecoder.decode(message.payload());      // throws TextConversionException
                _log.log(message.logLevelOrNull(),
                         String.format("<SENDER> %s: %s",
                                       msgType, Text.stripLast(text)));
            }
        } catch (TextConversionException e) {
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe(String.format(
                    "Peer sent a message but we failed to convert all " +
                    "characters in message. %s (%s)", e, message.toString()));
            }
            throw new RsyncProtocolException(e);
        }
    }

    private void handleMessageNoSend(int index)
    {
        try {
            if (index < 0) {
                throw new RsyncProtocolException(String.format(
                    "received illegal MSG_NO_SEND index: %d < 0", index));
            }
            _generator.purgeFile(null, index);
        } catch (InterruptedException e) {
            throw new RuntimeInterruptException(e);
        }
    }

    private Checksum.Header receiveChecksumHeader() throws ChannelException
    {
        return Connection.receiveChecksumHeader(_senderInChannel);
    }

    private void receiveFiles(Filelist fileList, Filelist.Segment firstSegment)
        throws ChannelException, InterruptedException
    {
        Filelist.Segment segment = firstSegment;
        int numSegmentsInProgress = 1;
        ConnectionState connectionState = new ConnectionState();
        boolean isEOF = !_isRecursive;

        while (connectionState.isTransfer()) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("num bytes available to read: %d",
                                        _senderInChannel.numBytesAvailable()));
            }

            final int index = _senderInChannel.decodeIndex();
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("Received index %d", index));
            }

            if (index == Filelist.DONE) {
                if (!_isRecursive && !fileList.isEmpty()) {
                    throw new IllegalStateException(
                        "received file list DONE when not recursive and file " +
                        "list is not empty: " + fileList);
                }
                numSegmentsInProgress--;
                if (numSegmentsInProgress <= 0 && fileList.isEmpty()) {
                    if (!isEOF) {
                        throw new IllegalStateException(
                            "got file list DONE with empty file list and at " +
                            "least all ouststanding segment deletions " +
                            "acknowledged but haven't received file list EOF");
                    }
                    connectionState.doTearDownStep();
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine("tearing down at phase " + connectionState);
                    }
                    _generator.sendSegmentDone(); // 3 after empty
                } else if (numSegmentsInProgress < 0 && !fileList.isEmpty()) {
                    throw new IllegalStateException(
                        "Received more acked deleted segments then we have " +
                        "sent to peer: " + fileList);
                }
            } else if (index == Filelist.EOF) {
                if (isEOF) {
                    throw new IllegalStateException("received duplicate file " +
                                                    "list EOF");
                }
                if (!_isRecursive) {
                    throw new IllegalStateException("Received file list EOF" +
                                                    " from peer while not " +
                                                    "doing incremental " +
                                                    "recursing");
                }
                if (fileList.isExpandable()) {
                    throw new IllegalStateException("Received file list EOF " +
                                                    "from peer while having " +
                                                    "an expandable file " +
                                                    "list: " + fileList);
                }
                isEOF = true;
            } else if (index < 0) {
                if (!_isRecursive) {
                    throw new IllegalStateException("Received negative file " +
                                                    "index from peer while " +
                                                    "not doing incremental " +
                                                    "recursing");
                }
                int directoryIndex = Filelist.OFFSET - index;
                FileInfo directory =
                    fileList.getStubDirectoryOrNull(directoryIndex);
                if (directory == null) {
                    throw new IllegalStateException(String.format(
                        "there is no stub directory for index %d",
                        directoryIndex));
                }
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                        "Receiving directory index %d is dir %s",
                        directoryIndex, directory.path()));
                }

                List<FileInfoStub> stubs = new LinkedList<>();
                _ioError |= receiveFileMetaDataInto(stubs);
                Filelist.SegmentBuilder builder =
                    new Filelist.SegmentBuilder(directory);
                _ioError |= extractFileMetadata(stubs, builder);
                segment = fileList.newSegment(builder);
                _generator.generateSegment(segment);
                numSegmentsInProgress++;
            } else if (index >= 0) {
                if (_isListOnly) {
                    throw new RsyncProtocolException(String.format(
                        "Error: received file index %d when listing files " +
                        "only", index));
                }

                final char iFlags = _senderInChannel.getChar();
                if (!Item.isValidItem(iFlags)) {
                    throw new IllegalStateException(String.format("got flags %d - not supported"));
                }

                if ((iFlags & Item.TRANSFER) == 0) {
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine("index " + index + " is not a transfer");
                    }
                    continue;
                }

                if (connectionState.isTearingDown()) { // NOTE: Originally was:  if (atTearDownPhase >= Consts.NUM_TEARDOWN_PHASES - 1) {
                    throw new RsyncProtocolException(
                        String.format("Error: wrong phase (%s)",
                                      connectionState));
                }

                FileInfo fileInfo = segment.getFileWithIndexOrNull(index);
                if (fileInfo == null) {
                    if (!_isRecursive) {
                        throw new RsyncProtocolException(String.format(
                            "Received invalid file index %d from peer",
                            index));
                    }
                    segment = fileList.getSegmentWith(index);
                    if (segment == null) {
                        throw new RsyncProtocolException(String.format(
                            "Received invalid file %d from peer",
                            index));
                    }
                    fileInfo = segment.getFileWithIndexOrNull(index);
                    assert fileInfo != null;
                }

                if (_log.isLoggable(Level.INFO)) {
                    _log.info(fileInfo.path().toString());
                }

                _stats.setNumTransferredFiles(_stats.numTransferredFiles() + 1);
                _stats.setTotalTransferredSize(_stats.totalTransferredSize() +
                                               fileInfo.attrs().size());

                if (fileInfo.isTransferred() && _log.isLoggable(Level.FINE)) {
                    _log.fine("Re-receiving " + fileInfo.path());
                }

                Checksum.Header checksumHeader = receiveChecksumHeader();
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("received peer checksum " + checksumHeader);
                }

                Path tempFile = null;
                try {
                    tempFile = Files.createTempFile(CustomFileSystem.getTempPath(fileInfo.path().getParent().toString()),
                                                    null, null);
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine("created tempfile " + tempFile);
                    }
                    matchData(segment, index, fileInfo, checksumHeader,
                              tempFile);
                } catch (IOException e) {
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(String.format(
                            "failed to create tempfile in %s: %s",
                            fileInfo.path().getParent(), e.getMessage()));
                    }
                    discardData(checksumHeader);
                    _senderInChannel.skip(Checksum.MAX_DIGEST_LENGTH);
                    _ioError |= IoError.GENERAL;
                    // TODO: send error message to peer
                    _generator.purgeFile(segment, index);
                } finally {
                    try {
                        // TODO: save temporary file when md5sum mismatches
                        // as next replica to use for this file, it should often
                        // be closer to what the sender has than our previous
                        // replica
                        if (tempFile != null) {
                            Files.deleteIfExists(tempFile);
                        }
                    } catch (IOException e) {
                        if (_log.isLoggable(Level.WARNING)) {
                            _log.warning(String.format(
                                "Warning: failed to remove tempfile %s: %s",
                                tempFile, e.getMessage()));
                        }
                    }
                }
            }
        }
    }

    private boolean isRemoteAndLocalFileIdentical(Path localFile,
                                                  MessageDigest md,
                                                  FileInfo fileInfo)
        throws ChannelException
    {
        long tempSize = localFile == null ? -1 : FileOps.sizeOf(localFile);
        byte[] md5sum = md.digest();
        byte[] peerMd5sum = new byte[md5sum.length];
        _senderInChannel.get(ByteBuffer.wrap(peerMd5sum));
        boolean isIdentical = tempSize == fileInfo.attrs().size() &&
                              Arrays.equals(md5sum, peerMd5sum);

        //isIdentical = isIdentical && Util.randomChance(0.25);

        if (_log.isLoggable(Level.FINE)) {
            if (isIdentical) {
                _log.fine(String.format("%s data received OK (remote and " +
                                        "local checksum is %s)",
                                        fileInfo.path(),
                                        MD5.md5DigestToString(md5sum)));
            } else {
                _log.fine(String.format("%s checksum/size mismatch : " +
                                        "our=%s (size=%d), peer=%s (size=%d)",
                                        fileInfo.path(),
                                        MD5.md5DigestToString(md5sum),
                                        tempSize,
                                        MD5.md5DigestToString(peerMd5sum),
                                        fileInfo.attrs().size()));
            }
        }
        return isIdentical;
    }

    private void moveTempfileToTarget(Path tempFile, Path target)
    {
        boolean isOK = FileOps.atomicMove(tempFile, target);
        if (!isOK) {
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning(String.format("Error: when moving temporary file" +
                                           " %s to %s", tempFile, target));
            }
            _ioError |= IoError.GENERAL;
        }
    }

    private void updateAttrsIfDiffer(Path path, RsyncFileAttributes targetAttrs)
        throws IOException
    {
        RsyncFileAttributes curAttrs = RsyncFileAttributes.stat(path);

        if (_isPreservePermissions && curAttrs.mode() != targetAttrs.mode()) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                    "updating file permissions %o -> %o on %s",
                    curAttrs.mode(), targetAttrs.mode(), path));
            }
            FileOps.setFileMode(path, targetAttrs.mode(),
                                LinkOption.NOFOLLOW_LINKS);
        }
        if (_isPreserveTimes && curAttrs.lastModifiedTime() != targetAttrs.lastModifiedTime()) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                    "updating mtime %d -> %d on %s",
                    curAttrs.lastModifiedTime(),
                    targetAttrs.lastModifiedTime(), path));
            }
            FileOps.setLastModifiedTime(path, targetAttrs.lastModifiedTime(),
                                        LinkOption.NOFOLLOW_LINKS);
        }
    }

    private void matchData(Filelist.Segment segment, int index,
                           FileInfo fileInfo, Checksum.Header checksumHeader,
                           Path tempFile)
        throws ChannelException, InterruptedException
    {
        MessageDigest md = MD5.newInstance();
        Path resultFile = mergeDataFromPeerAndReplica(fileInfo,
                                                      tempFile,
                                                      checksumHeader,
                                                      md);
        if (isRemoteAndLocalFileIdentical(resultFile, md, fileInfo)) {
            try {
                if (_isPreservePermissions || _isPreserveTimes) {
                    updateAttrsIfDiffer(resultFile, fileInfo.attrs());
                }
                if (!_isDeferredWrite || !resultFile.equals(fileInfo.path())) {
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine(String.format("moving %s -> %s",
                                                resultFile, fileInfo.path()));
                    }
                    moveTempfileToTarget(resultFile, fileInfo.path());
                }
            } catch (IOException e) {
                _ioError |= IoError.GENERAL;
                if (_log.isLoggable(Level.SEVERE)) {
                    _log.log(Level.SEVERE, "", e);
                }
            }
            _generator.purgeFile(segment, index);
        } else {
            if (fileInfo.isTransferred()) {
                _ioError |= IoError.GENERAL;
                try {
                    _generator.sendMessage(MessageCode.ERROR_XFER,
                                           String.format("%s (index %d) failed " +
                                                         "verification, update " +
                                                         "discarded\n",
                                                         fileInfo.path(), index));
                } catch (TextConversionException e) {
                    if (_log.isLoggable(Level.SEVERE)) {
                        _log.log(Level.SEVERE, "", e);
                    }
                }
                _generator.purgeFile(segment, index);
            } else {
                _generator.generateFile(segment, index, fileInfo);
                fileInfo.setIsTransferred();
            }
        }
    }

    private int receiveAndDecodeInt() throws ChannelException
    {
        return (int) receiveAndDecodeLong(1);
    }

    private long receiveAndDecodeLong(int minBytes) throws ChannelException
    {
        try {
            return IntegerCoder.decodeLong(_senderInChannel, minBytes);
        } catch (Exception e) {
            throw new ChannelException(e.getMessage());
        }
    }

    /**
     * @throws RsyncProtocolException if received file is invalid in some way
     */
    private int receiveFileMetaDataInto(List<FileInfoStub> builder)
        throws ChannelException
    {
        int ioError = 0;
        long numBytesRead = _senderInChannel.numBytesRead() -
                            _senderInChannel.numBytesPrefetched();

        while (true) {
            char flags = (char) _senderInChannel.getByte();
            if (flags == 0) {
                break;
            }
            if ((flags & TransmitFlags.EXTENDED_FLAGS) != 0) {
                flags |= _senderInChannel.getByte() << 8;
                if (flags == (TransmitFlags.EXTENDED_FLAGS |
                              TransmitFlags.IO_ERROR_ENDLIST)) {
                    ioError |= receiveAndDecodeInt();
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(String.format("peer process returned an " +
                                                   "I/O error (%d)", ioError));
                    }
                    break;
                }
            }
            byte[] pathNameBytes = receivePathNameBytes(flags);
            RsyncFileAttributes attrs = receiveRsyncFileAttributes(flags);
            String pathName =
                _characterDecoder.decodeOrNull(pathNameBytes);

            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("Receiving file information for %s: %s",
                                        pathName, attrs));
            }

            FileInfoStub stub = new FileInfoStub(pathName, pathNameBytes,
                                                 attrs);
            builder.add(stub);
        }

        long segmentSize = _senderInChannel.numBytesRead() -
                           _senderInChannel.numBytesPrefetched() - numBytesRead;
        _stats.setTotalFileListSize(_stats.totalFileListSize() + segmentSize);
        return ioError;
    }

    private int extractFileMetadata(List<FileInfoStub> stubs,
                                    Filelist.SegmentBuilder builder)
        throws InterruptedException
    {
        int ioError = 0;

        for (FileInfoStub stub : stubs) {

            String pathName = stub._pathName;
            byte[] pathNameBytes = stub._pathNameBytes;
            RsyncFileAttributes attrs = stub._attrs;
            FileInfo fileInfo = null;

            if (pathName == null) {
                ioError |= IoError.GENERAL;
                try {
                    _generator.sendMessage(MessageCode.ERROR,
                        String.format("Error: unable to decode path name " +
                                      "of %s using character set %s. " +
                                      "Result with illegal characters " +
                                      "replaced: %s\n",
                                      Text.bytesToString(pathNameBytes),
                                      _characterDecoder.charset(),
                                      new String(pathNameBytes,
                                                 _characterDecoder.charset())));
                } catch (TextConversionException e) {
                    if (_log.isLoggable(Level.SEVERE)) {
                        _log.log(Level.SEVERE, "", e);
                    }
                }
            } else if (!PathOps.isDirectoryStructurePreservable(pathName)) {    // TODO: implement support for user defined mapping of illegal characters
                ioError |= IoError.GENERAL;
                try {
                    _generator.sendMessage(MessageCode.ERROR,
                        String.format("Illegal file name. \"%s\" contains" +
                                      " this OS' path name separator " +
                                      "\"%s\" and cannot be stored and " +
                                      "later retrieved using the same " +
                                      "name again\n",
                                      pathName,
                                      Environment.PATH_SEPARATOR));
                } catch (TextConversionException e) {
                    if (_log.isLoggable(Level.SEVERE)) {
                        _log.log(Level.SEVERE, "", e);
                    }
                }
            } else {
                try {
                    Path relativePath = _pathResolver.relativePathOf(pathName);
                    Path fullPath = _pathResolver.fullPathOf(relativePath);

                    if (PathOps.isPathPreservable(fullPath)) {
                        fileInfo = new FileInfo(fullPath,
                                                relativePath,
                                                pathNameBytes,
                                                attrs);              // throws IllegalArgumentException but this is avoided due to previous checks

                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("Finished receiving " + fileInfo);
                        }
                    } else {
                        /* cygwin can obviously preserve trailing dots we'd also
                         * like to do that this is a security issue path/to/...
                         * would be resolved to path/to/, and could cause
                         * unexpected results where path/to/file. would be
                         * resolved to path/to/file */
                        ioError |= IoError.GENERAL;
                        _generator.sendMessage(MessageCode.ERROR,
                                    String.format("Unable to preserve file " +
                                                  "name for: \"%s\"\n",
                                                  pathName));
                    }
                } catch (InvalidPathException e) {
                    ioError |= IoError.GENERAL;
                    _generator.sendMessage(MessageCode.ERROR, e.getMessage());
                }
            }
            /* NOTE: we must keep the file regardless of any errors, or else
             * we'll have mismatching file list with sender */
            if (fileInfo == null) {
                fileInfo = new FileInfo(null, null, pathNameBytes, attrs);
            }
            builder.add(fileInfo);
        }
        return ioError;
    }


    /**
     * @throws RsyncProtocolException if received file is invalid in some way
     */
    private byte[] receivePathNameBytes(char xflags) throws ChannelException
    {
        int prefixNumBytes = 0;
        if ((xflags & TransmitFlags.SAME_NAME) != 0) {
            prefixNumBytes = 0xFF & _senderInChannel.getByte();
        }
        int suffixNumBytes;
        if ((xflags & TransmitFlags.LONG_NAME) != 0) {
            suffixNumBytes = receiveAndDecodeInt();
        } else {
            suffixNumBytes = 0xFF & _senderInChannel.getByte();
        }

        byte[] prevFileNameBytes = _fileInfoCache.getPrevFileNameBytes();
        byte[] fileNameBytes = new byte[prefixNumBytes + suffixNumBytes];
        Util.copyArrays(prevFileNameBytes, fileNameBytes, prefixNumBytes);
        _senderInChannel.get(fileNameBytes, prefixNumBytes, suffixNumBytes);
        _fileInfoCache.setPrevFileNameBytes(fileNameBytes);
        return fileNameBytes;
    }

    private RsyncFileAttributes receiveRsyncFileAttributes(char xflags)
        throws ChannelException
    {
        long fileSize = receiveAndDecodeLong(3);
        if (fileSize < 0) {
            throw new RsyncProtocolException(String.format(
                "received negative file size %d", fileSize));
        }

        long lastModified;
        if ((xflags & TransmitFlags.SAME_TIME) != 0) {
            lastModified = _fileInfoCache.getPrevLastModified();
        } else {
            lastModified = receiveAndDecodeLong(4);
            _fileInfoCache.setPrevLastModified(lastModified);
        }
        if (lastModified < 0) {
            throw new RsyncProtocolException(String.format(
                "received last modification time %d", lastModified));
        }

        int mode;
        if ((xflags & TransmitFlags.SAME_MODE) != 0) {
            mode = _fileInfoCache.getPrevMode();
        } else {
            mode = _senderInChannel.getInt();
            _fileInfoCache.setPrevMode(mode);
        }

        if ((xflags & TransmitFlags.SAME_UID) == 0) {
            throw new RsyncProtocolException("TransmitFlags.SAME_UID is " +
                                             "required");
        }
        if ((xflags & TransmitFlags.SAME_GID) == 0) {
            throw new RsyncProtocolException("TransmitFlags.SAME_GID is " +
                                             "required");
        }

        RsyncFileAttributes attrs = new RsyncFileAttributes(mode,
                                                            fileSize,
                                                            lastModified);      // throws IllegalArgumentException if fileSize or lastModified is negative, but we check for this earlier
        return attrs;
    }

    // FIXME: remove me, replace with combineDataToFile
    private void discardData(Checksum.Header checksumHeader)
        throws ChannelException
    {
        long sizeLiteral = 0;
        long sizeMatch = 0;
        while (true) {
            int token = _senderInChannel.getInt();
            if (token == 0) {
                break;
            } else if (token > 0) {
                int numBytes = token;
                _senderInChannel.skip(numBytes);
                sizeLiteral += numBytes;
            } else {
                final int blockIndex = - (token + 1);  // blockIndex >= 0 && blockIndex <= Integer.MAX_VALUE
                sizeMatch += sizeForChecksumBlock(blockIndex, checksumHeader);
            }
        }
        _stats.setTotalLiteralSize(_stats.totalLiteralSize() + sizeLiteral);
        _stats.setTotalMatchedSize(_stats.totalMatchedSize() + sizeMatch);
    }

    private Path mergeDataFromPeerAndReplica(FileInfo fileInfo,
                                             Path tempFile,
                                             Checksum.Header checksumHeader,
                                             MessageDigest md)
                                             throws ChannelException
    {
        assert fileInfo != null;
        assert tempFile != null;
        assert checksumHeader != null;
        assert md != null;

        try (SeekableByteChannel outFile = Files.newByteChannel(tempFile,
                                                    StandardOpenOption.WRITE)) {
            try (SeekableByteChannel replica =
            		Files.newByteChannel(fileInfo.path(), StandardOpenOption.READ)) {
                RsyncFileAttributes attrs =
                    RsyncFileAttributes.stat(fileInfo.path());
                if (attrs.isRegularFile()) {
                    boolean isIntact = combineDataToFile(replica, outFile,
                                                         checksumHeader, md);
                    if (isIntact) {
                        if (!attrs.equals(RsyncFileAttributes.statOrNull(fileInfo.path()))) {
                            if (_log.isLoggable(Level.WARNING)) {
                                _log.warning(String.format(
                                    "%s modified during verification",
                                    fileInfo.path()));
                            }
                            md.update((byte) 0);
                        }
                        return fileInfo.path();
                    }
                    return tempFile;
                } // else discard later
            } catch (NoSuchFileException e) {  // replica.open
                combineDataToFile(null, outFile, checksumHeader, md);
                return tempFile;
            }
        } catch (IOException e) {        // outFile.open
            // discard below
        }
        discardData(checksumHeader);
        return null;
    }

    // replica may be null
    private boolean combineDataToFile(SeekableByteChannel replica,
    								  SeekableByteChannel outFile,
                                      Checksum.Header checksumHeader,
                                      MessageDigest md)
        throws IOException, ChannelException
    {
        assert outFile != null;
        assert checksumHeader != null;
        assert md != null;

        boolean isIntact = _isDeferredWrite && replica != null;
        long sizeLiteral = 0;
        long sizeMatch = 0;
        int expectedIndex = 0;

        while (true) {
            final int token = _senderInChannel.getInt();
            if (token == 0) {
                break;
            }

            if (token < 0) {  // token correlates to a matching block index
                final int blockIndex = - (token + 1);  // blockIndex >= 0 && blockIndex <= Integer.MAX_VALUE

                if (_log.isLoggable(Level.FINEST)) {
                    _log.finest(String.format("got matching block index %d",
                                              blockIndex));
                }
                if (blockIndex > checksumHeader.chunkCount() - 1) {
                    throw new RsyncProtocolException(String.format(
                        "Received invalid block index from peer %d, which is " +
                        "out of range for the supposed number of blocks %d",
                        blockIndex, checksumHeader.chunkCount()));
                } else if (checksumHeader.blockLength() == 0) {
                    throw new RsyncProtocolException(String.format(
                        "Received a matching block index from peer %d when we" +
                        " never sent any blocks to peer (checksum " +
                        "blockLength = %d)",blockIndex, checksumHeader.blockLength()));
                } else if (replica == null) {
                    // or we could alternatively read zeroes from replica and have
                    // the correct file size in the end?
                    //
                    // i.e. generator sent file info to sender and sender
                    // replies with a match but now our replica is gone
                    continue;
                }

                sizeMatch += sizeForChecksumBlock(blockIndex, checksumHeader);

                if (isIntact) {
                    if (blockIndex == expectedIndex) { // if not identical to previous index we could possible try to see if the checksum are identical as a fallback attempt
                        expectedIndex++;
                        continue;
                    }
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine(String.format("defer-write disabled since " +
                                                "%d != %d",
                                                blockIndex, expectedIndex));
                    }
                    isIntact = false;
                    copyBlockRange(expectedIndex, checksumHeader, replica,
                                   outFile, md);
                }
                matchReplica(blockIndex, checksumHeader, replica, outFile, md);
            } else if (token > 0) { // receive non-matched literal data from peer:
                if (isIntact) {
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine(String.format("defer-write disabled since " +
                                                "we got literal data %d",
                                                token));
                    }
                    isIntact = false;
                    copyBlockRange(expectedIndex, checksumHeader, replica,
                                   outFile, md);
                }
                int length = token;
                sizeLiteral += length;
                if (outFile != null) {
                    copyRemoteBlocks(outFile, length, md);
                }
            }
        }

        if (isIntact && expectedIndex != checksumHeader.chunkCount()) { // rare truncation of multiples of checksum blocks
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("defer-write disabled since " +
                                        "expectedIndex %d != " +
                                        "checksumHeader.chunkCount() %d",
                                        expectedIndex,
                                        checksumHeader.chunkCount()));
            }
            isIntact = false;
            copyBlockRange(expectedIndex, checksumHeader, replica, outFile, md);
        }
        if (isIntact) {
            verifyBlockRange(expectedIndex, checksumHeader, replica, md);
        }

        if (_log.isLoggable(Level.FINE)) {
            if (_isDeferredWrite && replica != null && !isIntact) {
                _log.fine("deferred write disabled");
            }
            _log.fine(String.format("total bytes = %d, num matched bytes = " +
                                    "%d, num literal bytes = %d, %f%% match",
                                    sizeMatch + sizeLiteral,
                                    sizeMatch, sizeLiteral,
                                    100 * sizeMatch /
                                          (float) (sizeMatch + sizeLiteral)));
        }
        _stats.setTotalLiteralSize(_stats.totalLiteralSize() + sizeLiteral);
        _stats.setTotalMatchedSize(_stats.totalMatchedSize() + sizeMatch);
        return isIntact;
    }

    private void copyRemoteBlocks(SeekableByteChannel outFile, int length,
                                  MessageDigest md)
        throws ChannelException
    {
        // TODO: possibly skip writing out to file if replica is not OK
        int bytesReceived = 0;
        while (bytesReceived < length) {
            int chunkSize = Math.min(INPUT_CHANNEL_BUF_SIZE,
                                     length - bytesReceived);
            ByteBuffer literalData = _senderInChannel.get(chunkSize);
            bytesReceived += chunkSize;
            if (outFile != null) {
                literalData.mark();
                writeOut(outFile, literalData);
                literalData.reset();
            }
            md.update(literalData);
        }
    }

    private void verifyBlockRange(int endIndex,
                                  Checksum.Header checksumHeader,
                                  SeekableByteChannel replica,
                                  MessageDigest md)
        throws IOException
    {
        copyBlockRange(endIndex, checksumHeader, replica, null, md);
    }

    private void copyBlockRange(int endIndex,
                                Checksum.Header checksumHeader,
                                SeekableByteChannel replica,
                                SeekableByteChannel outFile,
                                MessageDigest md)
        throws IOException
    {
        for (int i = 0; i < endIndex; i++) {
            matchReplica(i, checksumHeader, replica, outFile, md);
        }
    }

    private void matchReplica(int blockIndex,
                              Checksum.Header checksumHeader,
                              SeekableByteChannel replica,
                              SeekableByteChannel outFile,
                              MessageDigest md)
        throws IOException
    {
        ByteBuffer replicaBuf =
            ByteBuffer.allocate(sizeForChecksumBlock(blockIndex,
                                                     checksumHeader));
        long fileOffset = blockIndex * checksumHeader.blockLength();
        replica.position(fileOffset);
        int bytesRead = replica.read(replicaBuf);
        if (replicaBuf.hasRemaining()) {
            throw new IllegalStateException(String.format(
                "truncated read from replica (%s), read %d " +
                    "bytes but expected %d more bytes",
                    replica, bytesRead, replicaBuf.remaining()));
        }
        replicaBuf.flip();

        // TODO: clean this up, not good
        if (outFile != null) {
            writeOut(outFile, replicaBuf);
            replicaBuf.rewind();
        }
        md.update(replicaBuf);
    }

    private int sizeForChecksumBlock(int blockIndex,
                                     Checksum.Header checksumHeader)
    {
        if (blockIndex == checksumHeader.chunkCount() - 1 &&
            checksumHeader.remainder() != 0) {
            return checksumHeader.remainder();
        }
        return checksumHeader.blockLength();
    }

    // FIXME: handle out of space sitation without a stack trace
    private void writeOut(SeekableByteChannel outFile, ByteBuffer src)
    {
        try {
            outFile.write(src); // NOTE: might notably fail due to running out of disk space
            if (src.hasRemaining()) {
                throw new IllegalStateException(String.format(
                    "truncated write to outFile (%s), returned %d bytes, " +
                    "expected %d more bytes",
                    outFile, src.position(), src.remaining()));
            }
        } catch (IOException e) {
            // native exists immediately if this happens, and so do we:
            throw new RuntimeException(e);
        }
    }

    // FIXME: code duplication with Receiver
    public void readAllMessagesUntilEOF() throws ChannelException
    {
        try {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("reading final messages until EOF");
            }
            byte dummy = _senderInChannel.getByte(); // dummy read to get any final messages from peer
            // we're not expected to get this far, getByte should throw NetworkEOFException
            throw new RsyncProtocolException(
                String.format("Peer sent invalid data during connection tear " +
                              "down (%d)", dummy));
        } catch (ChannelEOFException e) {
            // It's OK, we expect EOF without having received any data
        }
    }
}
