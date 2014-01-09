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
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
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
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.text.TextConversionException;
import com.github.perlundq.yajsync.text.TextDecoder;
import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.FileOps;
import com.github.perlundq.yajsync.util.MD5;
import com.github.perlundq.yajsync.util.PathOps;
import com.github.perlundq.yajsync.util.RuntimeInterruptException;
import com.github.perlundq.yajsync.util.Util;

public class Receiver implements MessageHandler
{
    private static final Logger _log =
        Logger.getLogger(Receiver.class.getName());

    private static final int INPUT_CHANNEL_BUF_SIZE = 8 * 1024;
    private final FileInfoCache _fileInfoCache = new FileInfoCache();
    private final Generator _generator;
    private final Path _targetDir;
    private final RsyncInChannel _senderInChannel;
    private final Statistics _stats = new Statistics();
    private final TextDecoder _characterDecoder;
    private boolean _isRecursive;
    private boolean _isListOnly;
    private boolean _isPreserveTimes;
    private boolean _isDeferredWrite;
    private int _ioError;

    public Receiver(Generator generator,
                    ReadableByteChannel in,
                    Path targetDir,
                    Charset charset)
    {
        assert targetDir.isAbsolute();
        _senderInChannel = new RsyncInChannel(in,
                                              this,
                                              INPUT_CHANNEL_BUF_SIZE);
        _characterDecoder = TextDecoder.newStrict(charset);
        _targetDir = targetDir.normalize();
        _generator = generator;
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
    
    public void setIsDeferredWrite(boolean isDeferredWrite)
    {
        _isDeferredWrite = isDeferredWrite;
    }
    
    public boolean isDeferredWrite()
    {
        return _isDeferredWrite;
    }
    
    
    public boolean receive(boolean sendFilterRules,
                           boolean receiveStatistics,
                           boolean exitEarlyIfEmptyList)
        throws ChannelException
    {
        try {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("Receiver.transfer:");
            }
            if (sendFilterRules) {
                sendEmptyFilterRules();
            }

            Filelist.SegmentBuilder builder = new Filelist.SegmentBuilder(null);
            _ioError |= receiveFileMetaDataInto(builder);

            if (builder.size() == 0 && exitEarlyIfEmptyList) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("empty file list - exiting early");
                }
                // NOTE: we never _receive_ any statistics if initial file list is empty
                return _ioError == 0;
            }
            Filelist fileList = new ConcurrentFilelist(_isRecursive);           // FIXME: move out
            _generator.setFileList(fileList);                                   // FIXME: move out
            Filelist.Segment segment = fileList.newSegment(builder);
            _generator.generateSegment(segment);
            receiveFiles(fileList, segment);
            _stats.setNumFiles(fileList.numFiles());
            if (receiveStatistics) {
                receiveStatistics(_stats);
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
            return _ioError == 0;
        } catch (InterruptedException | RuntimeInterruptException e) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("Receiver was interrupted");
            }
            return false;
        }
    }

    private void receiveStatistics(Statistics stats) throws ChannelException
    {
        long totalWritten = receiveAndDecodeLong(3);
        long totalRead = receiveAndDecodeLong(3);
        long totalFileSize = receiveAndDecodeLong(3);
        long fileListBuildTime = receiveAndDecodeLong(3);
        long fileListTransferTime = receiveAndDecodeLong(3);
        stats.setFileListBuildTime(fileListBuildTime);
        stats.setFileListTransferTime(fileListTransferTime);
        stats.setTotalRead(totalRead);
        stats.setTotalFileSize(totalFileSize);
        stats.setTotalWritten(totalWritten);
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

    @Override
    public void handleMessage(Message message)
    {
        switch (message.header().messageType()) {
        case IO_ERROR:
            int ioError = message.payload().getInt();
            _ioError |= ioError;
            break;
        case NO_SEND:
            int index = message.payload().getInt();
            handleMessageNoSend(index);
            break;
        case DATA:
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

    private void printMessage(Message message)
    {
        try {
            ByteBuffer payload = message.payload();
            String text = _characterDecoder.decode(payload);                    // throws TextConversionException
            MessageCode msgType = message.header().messageType();
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("<SENDER> %s: %s",
                    msgType, Text.stripLast(text)));
            }
            if (msgType.equals(MessageCode.ERROR_XFER)) {
                _ioError |= IoError.TRANSFER;                        // this is not what native does though - it stores it in a separate variable called got_xfer_error
            }
        } catch (TextConversionException e) {
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe(String.format(
                    "Peer sent a message but we failed to convert all " +
                    "characters in message. %s (%s)", e, message.toString()));
            }
            _ioError |= IoError.GENERAL;
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
                Filelist.SegmentBuilder segmentBuilder =
                    new Filelist.SegmentBuilder(directory);
                _ioError |= receiveFileMetaDataInto(segmentBuilder);
                segment = fileList.newSegment(segmentBuilder);
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
                    tempFile = Files.createTempFile(fileInfo.path().getParent(),
                                                    null, null);
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine("created tempfile " + tempFile);
                    }
                    matchData(segment, index, fileInfo, checksumHeader,
                              tempFile);
                } catch (IOException e) {
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(String.format(
                            "failed to create tempfile for %s: %s",
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

    private void updateFileAttributes(Path path, RsyncFileAttributes attrs)
    {
        if (_isPreserveTimes) {
            try {
                FileOps.setFileAttributes(path, attrs);
            } catch (IOException e) {
                if (_log.isLoggable(Level.WARNING)) {
                    _log.warning(String.format(
                        "failed to set attributes on %s: %s",
                        path, e.getMessage()));
                }
                _ioError |= IoError.GENERAL;
            }
        }
    }

    private void moveTempfileToTarget(Path tempFile, Path target)
    {
        if (_log.isLoggable(Level.FINE)) {
            _log.fine(String.format("Setting correct attributes and moving " +
                                    "%s -> %s", tempFile, target));
        }
        boolean isOK = FileOps.atomicMove(tempFile, target);
        if (!isOK) {
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning(String.format("Error: when moving temporary file" +
                                           " %s to %s", tempFile, target));
            }
            _ioError |= IoError.GENERAL;
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
            updateFileAttributes(resultFile, fileInfo.attrs());
            if (!_isDeferredWrite || !resultFile.equals(fileInfo.path())) {
                moveTempfileToTarget(resultFile, fileInfo.path());
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
    private int receiveFileMetaDataInto(Filelist.SegmentBuilder builder)
        throws ChannelException, InterruptedException
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
                        _log.warning(String.format(
                            "peer process returned an I/O error (%d)",
                            ioError));
                    }
                    break;
                }
            }

            FileInfo fileInfo = receiveFileMetaData(flags);                     // throws RsyncProtocolException if file is invalid in some way 
            // NOTE: fileInfo.path() is always null here - we resolve it fully
            // later
            
            String pathName = 
                _characterDecoder.decodeOrNull(fileInfo.pathNameBytes());
            
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("Receiving file information for %s: %s",
                          pathName, fileInfo));
            }
            
            if (pathName == null) {
                ioError |= IoError.GENERAL;
                try {
                    _generator.sendMessage(MessageCode.ERROR,
                        String.format("Error: unable to decode path name " +
                                      "of %s using character set %s. " +
                                      "Result with illegal characters " +
                                      "replaced: %s\n",
                                      Text.bytesToString(fileInfo.pathNameBytes()),
                                      _characterDecoder.charset(),
                                      new String(fileInfo.pathNameBytes(),
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
                    Path relativePath = Paths.get(pathName);                    // throws InvalidPathException
                    if (relativePath.isAbsolute()) {
                        throw new RsyncSecurityException(relativePath +
                                                         " is absolute");
                    }
                    Path fullPath =
                        _targetDir.resolve(relativePath).normalize();
                    if (!fullPath.startsWith(_targetDir)) {
                        throw new RsyncSecurityException(String.format(
                            "%s is outside of receiver destination dir %s",
                            fullPath, _targetDir));
                    }
                    Path normalizedRelativePath =
                        PathOps.normalizeStrict(relativePath);
                    if (PathOps.isPathPreservable(fullPath)) {
                        fileInfo = new FileInfo(fullPath,
                                                normalizedRelativePath,
                                                fileInfo.pathNameBytes(),
                                                fileInfo.attrs());              // throws IllegalArgumentException but this is avoided due to previous checks

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
            builder.add(fileInfo);
        }
        
        long segmentSize = _senderInChannel.numBytesRead() -
                           _senderInChannel.numBytesPrefetched() - numBytesRead;
        _stats.setTotalFileListSize(_stats.totalFileListSize() + segmentSize);
        return ioError;
    }

    /**
     * @throws RsyncProtocolException if received file is invalid in some way
     */
    private FileInfo receiveFileMetaData(char xflags) throws ChannelException
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

        if (prefixNumBytes + suffixNumBytes > PathOps.MAX_PATH_NAME_LENGTH) {
            throw new RsyncProtocolException(
                String.format("Error: fileName Length is too large: " +
                              "%d + %d = %d (limit is %d)",
                              prefixNumBytes, suffixNumBytes,
                              prefixNumBytes + suffixNumBytes,
                              PathOps.MAX_PATH_NAME_LENGTH)); // we use the same limit as native, even if local filesystem could possibly support longer names
        }

        byte[] prevFileNameBytes = _fileInfoCache.getPrevFileNameBytes();
        byte[] fileNameBytes = new byte[prefixNumBytes + suffixNumBytes];
        Util.copyArrays(prevFileNameBytes, fileNameBytes, prefixNumBytes);
        _senderInChannel.get(fileNameBytes, prefixNumBytes, suffixNumBytes);
        _fileInfoCache.setPrevFileNameBytes(fileNameBytes);

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
        try {
            return new FileInfo(null, null, fileNameBytes, attrs);              // throws IllegalArgumentException
        } catch (IllegalArgumentException e) {
            throw new RsyncProtocolException(e);
        }
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

        try (FileChannel outFile = FileChannel.open(tempFile,
                                                    StandardOpenOption.WRITE)) {
            try (FileChannel replica =
                   FileChannel.open(fileInfo.path(), StandardOpenOption.READ)) {
                RsyncFileAttributes attrs =
                    RsyncFileAttributes.stat(fileInfo.path());
                if (attrs.isRegularFile()) {
                    boolean isIntact = combineDataToFile(replica, outFile,
                                                         checksumHeader, md);
                    isIntact = isIntact && attrs.equals(RsyncFileAttributes.statOrNull(fileInfo.path()));
                    return isIntact ? fileInfo.path() : tempFile;
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
    private boolean combineDataToFile(FileChannel replica,
                                      FileChannel outFile,
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
                    isIntact = false;
                    copyBlockRange(expectedIndex, checksumHeader, replica,
                                   outFile, md);
                }
                matchReplica(blockIndex, checksumHeader, replica, outFile, md);
            } else if (token > 0) { // receive non-matched literal data from peer:
                if (isIntact) {
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
            isIntact = false;
            copyBlockRange(expectedIndex, checksumHeader, replica, outFile, md);
        }
        if (isIntact) {
            verifyBlockRange(expectedIndex, checksumHeader, replica, md);
        }

        if (_log.isLoggable(Level.FINE)) {
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

    private void copyRemoteBlocks(FileChannel outFile, int length,
                                  MessageDigest md)
        throws ChannelException
    {
        // TODO: possibly skip writing out to file if replica is not OK
        int bytesReceived = 0;
        while (bytesReceived < length) {
            int chunkSize = Math.min(1024, length - bytesReceived);
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
                                  FileChannel replica,
                                  MessageDigest md)
        throws IOException
    {
        copyBlockRange(endIndex, checksumHeader, replica, null, md);
    }

    private void copyBlockRange(int endIndex,
                                Checksum.Header checksumHeader,
                                FileChannel replica,
                                FileChannel outFile,
                                MessageDigest md)
        throws IOException
    {
        for (int i = 0; i < endIndex; i++) {
            matchReplica(i, checksumHeader, replica, outFile, md);
        }
    }

    private void matchReplica(int blockIndex,
                              Checksum.Header checksumHeader,
                              FileChannel replica,
                              FileChannel outFile,
                              MessageDigest md)
        throws IOException
    {
        ByteBuffer replicaBuf =
            ByteBuffer.allocate(sizeForChecksumBlock(blockIndex,
                                                     checksumHeader));
        long fileOffset = blockIndex * checksumHeader.blockLength();
        int bytesRead = replica.read(replicaBuf, fileOffset);
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
    private void writeOut(FileChannel outFile, ByteBuffer src)
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
