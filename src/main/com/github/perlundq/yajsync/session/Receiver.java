/*
 * Processing of incoming file lists and file data from Sender
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
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.channels.ChannelEOFException;
import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.channels.Message;
import com.github.perlundq.yajsync.channels.MessageCode;
import com.github.perlundq.yajsync.channels.MessageHandler;
import com.github.perlundq.yajsync.channels.RsyncInChannel;
import com.github.perlundq.yajsync.filelist.AbstractPrincipal;
import com.github.perlundq.yajsync.filelist.DeviceInfo;
import com.github.perlundq.yajsync.filelist.FileInfo;
import com.github.perlundq.yajsync.filelist.Filelist;
import com.github.perlundq.yajsync.filelist.Group;
import com.github.perlundq.yajsync.filelist.RsyncFileAttributes;
import com.github.perlundq.yajsync.filelist.SymlinkInfo;
import com.github.perlundq.yajsync.filelist.User;
import com.github.perlundq.yajsync.io.AutoDeletable;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.text.TextConversionException;
import com.github.perlundq.yajsync.text.TextDecoder;
import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.FileOps;
import com.github.perlundq.yajsync.util.MD5;
import com.github.perlundq.yajsync.util.PathOps;
import com.github.perlundq.yajsync.util.RuntimeInterruptException;
import com.github.perlundq.yajsync.util.Util;

public class Receiver implements RsyncTask, MessageHandler
{
    public static class Builder
    {
        private final Generator _generator;
        private final ReadableByteChannel _in;
        private final String _targetPathName;
        private boolean _isDeferWrite;
        private boolean _isExitAfterEOF;
        private boolean _isExitEarlyIfEmptyList;
        private boolean _isReceiveStatistics;
        private boolean _isSafeFileList = true;
        private FilterMode _filterMode = FilterMode.NONE;

        public Builder(Generator generator, ReadableByteChannel in,
                       String targetPathName)
        {
            assert generator != null;
            assert in != null;
            assert targetPathName != null;
            assert Paths.get(targetPathName).isAbsolute() : targetPathName;
            _generator = generator;
            _in = in;
            _targetPathName = targetPathName;
        }

        public static Builder newServer(Generator generator,
                                        ReadableByteChannel in,
                                        String targetPathName)
        {
            return new Builder(generator, in, targetPathName).
                    isReceiveStatistics(false).
                    isExitEarlyIfEmptyList(false).
                    isExitAfterEOF(false);
        }

        public static Builder newClient(Generator generator,
                                        ReadableByteChannel in,
                                        String targetPathName)
        {
            return new Builder(generator, in, targetPathName).
                    isReceiveStatistics(true).
                    isExitEarlyIfEmptyList(true).
                    isExitAfterEOF(true);
        }

        public Builder isDeferWrite(boolean isDeferWrite)
        {
            _isDeferWrite = isDeferWrite;
            return this;
        }

        public Builder isExitAfterEOF(boolean isExitAfterEOF)
        {
            _isExitAfterEOF = isExitAfterEOF;
            return this;
        }

        public Builder isExitEarlyIfEmptyList(boolean isExitEarlyIfEmptyList)
        {
            _isExitEarlyIfEmptyList = isExitEarlyIfEmptyList;
            return this;
        }

        public Builder isReceiveStatistics(boolean isReceiveStatistics)
        {
            _isReceiveStatistics = isReceiveStatistics;
            return this;
        }

        public Builder isSafeFileList(boolean isSafeFileList)
        {
            _isSafeFileList = isSafeFileList;
            return this;
        }

        public Builder filterMode(FilterMode filterMode)
        {
            assert filterMode != null;
            _filterMode = filterMode;
            return this;
        }

        public Receiver build()
        {
            return new Receiver(this);
        }
    }

    @SuppressWarnings("serial")
    private class PathResolverException extends Exception {
        public PathResolverException(String msg) {
            super(msg);
        }
    }

    private static class FileInfoStub {
        private final String _pathName;
        private final byte[] _pathNameBytes;
        private RsyncFileAttributes _attrs;
        private Path _symlinkTargetOrNull;
        private int _major = -1;
        private int _minor = -1;

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

    private static final int INPUT_CHANNEL_BUF_SIZE = 8 * 1024;
    private static final Logger _log =
        Logger.getLogger(Receiver.class.getName());

    private final BitSet _transferred = new BitSet();
    private final boolean _isDeferWrite;
    private final boolean _isExitAfterEOF;
    private final boolean _isExitEarlyIfEmptyList;
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
    private final boolean _isReceiveStatistics;
    private final boolean _isSafeFileList;
    private final FileInfoCache _fileInfoCache = new FileInfoCache();
    private final FileSelection _fileSelection;
    private final FilterMode _filterMode;
    private final Generator _generator;
    private final Map<Integer, User> _uidUserMap = new HashMap<>();
    private final Map<Integer, Group> _gidGroupMap = new HashMap<>();
    private final RsyncInChannel _in;
    private final Statistics _stats = new Statistics();
    private final String _targetPathName;
    private final TextDecoder _characterDecoder;

    private int _ioError;
    private PathResolver _pathResolver;

    private Receiver(Builder builder)
    {
        _isDeferWrite = builder._isDeferWrite;
        _isExitAfterEOF = builder._isExitAfterEOF;
        _isExitEarlyIfEmptyList = builder._isExitEarlyIfEmptyList;
        _isReceiveStatistics = builder._isReceiveStatistics;
        _isSafeFileList = builder._isSafeFileList;
        _generator = builder._generator;
        _isInterruptible = _generator.isInterruptible();
        _isListOnly = _generator.isListOnly();
        _isPreserveDevices = _generator.isPreserveDevices();
        _isPreserveLinks = _generator.isPreserveLinks();
        _isPreservePermissions = _generator.isPreservePermissions();
        _isPreserveSpecials = _generator.isPreserveSpecials();
        _isPreserveTimes = _generator.isPreserveTimes();
        _isPreserveUser = _generator.isPreserveUser();
        _isPreserveGroup = _generator.isPreserveGroup();
        _isNumericIds = _generator.isNumericIds();
        _fileSelection = _generator.fileSelection();
        _filterMode = builder._filterMode;
        _in = new RsyncInChannel(builder._in, this, INPUT_CHANNEL_BUF_SIZE);
        _targetPathName = builder._targetPathName;
        _characterDecoder = TextDecoder.newStrict(_generator.charset());
    }

    @Override
    public boolean isInterruptible()
    {
        return _isInterruptible;
    }

    @Override
    public void closeChannel() throws ChannelException
    {
        _in.close();
    }

    @Override
    public Boolean call() throws RsyncException, InterruptedException
    {
        try {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("Receiver.receive(targetPathName=%s, " +
                                        "isDeferWrite=%s," +
                                        " isListOnly=%s, isPreserveTimes=%s, " +
                                        "fileSelection=%s, " +
                                        "receiveStatistics=%s, " +
                                        "exitEarlyIfEmptyList=%s, " +
                                        "filterMode=%s",
                                        _targetPathName, _isDeferWrite,
                                        _isListOnly, _isPreserveTimes,
                                        _fileSelection,
                                        _isReceiveStatistics,
                                        _isExitEarlyIfEmptyList,
                                        _filterMode));
            }
            if (_filterMode == FilterMode.SEND) {
                sendEmptyFilterRules();
            } else if (_filterMode == FilterMode.RECEIVE) {
                String rules = receiveFilterRules();
                if (rules.length() > 0) {
                    throw new RsyncProtocolException(String.format(
                            "Received a list of filter rules of length %d " +
                            "from peer, this is not yet supported (%s)",
                            rules.length(), rules));
                }
            }

            if (!_isNumericIds && _fileSelection == FileSelection.RECURSE) {
                if (_isPreserveUser) {
                    _uidUserMap.put(User.ROOT.id(), User.ROOT);
                }
                if (_isPreserveGroup) {
                    _gidGroupMap.put(Group.ROOT.id(), Group.ROOT);
                }
            }

            List<FileInfoStub> stubs = new LinkedList<>();
            _ioError |= receiveFileMetaDataInto(stubs);

            if (!_isNumericIds && _fileSelection != FileSelection.RECURSE) {
                if (_isPreserveUser) {
                    Map<Integer, User> uidUserMap = receiveUserList();
                    uidUserMap.put(User.ROOT.id(), User.ROOT);
                    addUserNameToStubs(uidUserMap, stubs);
                }
                if (_isPreserveGroup) {
                    Map<Integer, Group> gidGroupMap = receiveGroupList();
                    gidGroupMap.put(Group.ROOT.id(), Group.ROOT);
                    addGroupNameToStubs(gidGroupMap, stubs);
                }
            }

            if (stubs.size() == 0 && _isExitEarlyIfEmptyList) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("empty file list - exiting early");
                }
                // NOTE: we never _receive_ any statistics if initial file list
                // is empty
                if (_isExitAfterEOF) {
                    readAllMessagesUntilEOF();
                }
                return _ioError == 0;
            }

            // throws InvalidPathException
            Path targetPath = PathOps.get(_targetPathName);
            // throws PathResolverException
            _pathResolver = getPathResolver(targetPath, stubs);
            if (_log.isLoggable(Level.FINER)) {
                _log.finer("Path Resolver: " + _pathResolver.toString());
            }
            Filelist.SegmentBuilder builder = new Filelist.SegmentBuilder(null);
            _ioError |= extractFileMetadata(stubs, builder);

            Filelist fileList = _generator.fileList();
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
                        _in.numBytesRead()));
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
        } catch (InvalidPathException e) {
            throw new RsyncException(String.format(
                "illegal target path name %s: %s", _targetPathName, e));
        } catch (PathResolverException e) {
            throw new RsyncException(e);
        } finally {
            _generator.stop();
        }
    }

    /**
     * @throws RsyncProtocolException if failing to decode the filter rules
     */
    private String receiveFilterRules() throws ChannelException
    {
        try {
            int numBytesToRead = _in.getInt();
            ByteBuffer buf = _in.get(numBytesToRead);
            String filterRules = _characterDecoder.decode(buf);
            return filterRules;
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        }
    }


    /**
     * @throws RsyncProtocolException if user name is the empty string
     */
    private Map<Integer, User> receiveUserList() throws ChannelException
    {
        Map<Integer, User> users = new HashMap<>();
        while (true) {
            int uid = receiveUserId();
            boolean isDone = uid == 0;
            if (isDone) {
                return users;
            }
            String userName = receiveUserName();
            User user = new User(userName, uid);
            users.put(uid, user);
        }
    }

    /**
     * @throws RsyncProtocolException if group name is the empty string
     */
    private Map<Integer, Group> receiveGroupList() throws ChannelException
    {
        Map<Integer, Group> groups = new HashMap<>();
        while (true) {
            int gid = receiveGroupId();
            boolean isDone = gid == 0;
            if (isDone) {
                return groups;
            }
            String groupName = receiveGroupName();
            Group group = new Group(groupName, gid);
            groups.put(gid, group);
        }
    }

    private void addUserNameToStubs(Map<Integer, User> uidUserMap,
                                    List<FileInfoStub> stubs)
    {
        for (FileInfoStub stub : stubs) {
            RsyncFileAttributes incompleteAttrs = stub._attrs;
            boolean isComplete = incompleteAttrs.user().name().length() > 0;
            if (isComplete) {
                throw new RsyncProtocolException(String.format(
                    "expected user name of %s to be the empty string",
                    incompleteAttrs));
            }
            User completeUser = uidUserMap.get(incompleteAttrs.user().id());
            if (completeUser != null) {
                RsyncFileAttributes completeAttrs =
                    new RsyncFileAttributes(incompleteAttrs.mode(),
                                            incompleteAttrs.size(),
                                            incompleteAttrs.lastModifiedTime(),
                                            completeUser,
                                            incompleteAttrs.group());
                stub._attrs = completeAttrs;
            }
        }
    }

    private void addGroupNameToStubs(Map<Integer, Group> gidGroupMap,
                                     List<FileInfoStub> stubs)
    {
        for (FileInfoStub stub : stubs) {
            RsyncFileAttributes incompleteAttrs = stub._attrs;
            boolean isComplete = incompleteAttrs.group().name().length() > 0;
            if (isComplete) {
                throw new RsyncProtocolException(String.format(
                        "expected group name of %s to be the empty string",
                        incompleteAttrs));
            }
            Group completeGroup = gidGroupMap.get(incompleteAttrs.group().id());
            if (completeGroup != null) {
                RsyncFileAttributes completeAttrs =
                    new RsyncFileAttributes(incompleteAttrs.mode(),
                                            incompleteAttrs.size(),
                                            incompleteAttrs.lastModifiedTime(),
                                            incompleteAttrs.user(),
                                            completeGroup);
                stub._attrs = completeAttrs;
            }
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
        try {
            // throws IOException
            RsyncFileAttributes attrs =
                RsyncFileAttributes.statIfExists(targetPath);

            boolean isTargetExisting = attrs != null;
            boolean isTargetExistingDir =
                isTargetExisting && attrs.isDirectory();
            boolean isTargetExistingFile =
                isTargetExisting && !attrs.isDirectory();
            boolean isSourceSingleFile =
                stubs.size() == 1 && !stubs.get(0)._attrs.isDirectory();
            boolean isTargetNonExistingFile =
                !isTargetExisting && !targetPath.endsWith(PathOps.DOT_DIR);

            if (_log.isLoggable(Level.FINER)) {
                _log.finer(String.format(
                        "targetPath=%s attrs=%s isTargetExisting=%s " +
                        "isSourceSingleFile=%s " +
                        "isTargetNonExistingFile=%s " +
                        "#stubs=%d",
                        targetPath, attrs, isTargetExisting,
                        isSourceSingleFile,
                        isTargetNonExistingFile, stubs.size()));
            }
            if (_log.isLoggable(Level.FINEST)) {
                _log.finest(stubs.toString());
            }

            // -> targetPath
            if (isSourceSingleFile && isTargetNonExistingFile ||
                isSourceSingleFile && isTargetExistingFile)
            {
                return new PathResolver() {
                    @Override public Path relativePathOf(String pathName) {
                        return Paths.get(stubs.get(0)._pathName);
                    }
                    @Override public Path fullPathOf(Path relativePath) {
                        return targetPath;
                    }
                    @Override public String toString() {
                        return "PathResolver(Single Source)";
                    }
                };
            }
            // -> targetPath/*
            if (isTargetExistingDir || !isTargetExisting) {
                if (!isTargetExisting) {
                    if (_log.isLoggable(Level.FINER)) {
                        _log.finer("creating directory (with parents) " +
                                   targetPath);
                    }
                    Files.createDirectories(targetPath);
                }
                return new PathResolver() {
                    @Override public Path relativePathOf(String pathName) {
                        // throws InvalidPathException
                        Path relativePath = Paths.get(pathName);
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
                    @Override public String toString() {
                        return "PathResolver(Complex)";
                    }
                };
            }

            if (isTargetExisting && !attrs.isDirectory() &&
                !attrs.isRegularFile() && !attrs.isSymbolicLink())
            {
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
            _generator.disableDelete();
            break;
        case NO_SEND:
            int index = message.payload().getInt();
            handleMessageNoSend(index);
            break;
        case ERROR:                         // store this as an IoError.TRANSFER
        case ERROR_XFER:
            _ioError |= IoError.TRANSFER;
            _generator.disableDelete();     // NOTE: fall through
        case INFO:
        case WARNING:
        case LOG:
            if (_log.isLoggable(message.logLevelOrNull())) {
                printMessage(message);
            }
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
            // throws TextConversionException
            String text = _characterDecoder.decode(message.payload());
            _log.log(message.logLevelOrNull(),
                    String.format("<SENDER> %s: %s", msgType,
                                  Text.stripLast(text)));
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
        return Connection.receiveChecksumHeader(_in);
    }

    private void receiveFiles(Filelist fileList, Filelist.Segment firstSegment)
        throws ChannelException, InterruptedException
    {
        Filelist.Segment segment = firstSegment;
        int numSegmentsInProgress = 1;
        ConnectionState connectionState = new ConnectionState();
        boolean isEOF = _fileSelection != FileSelection.RECURSE;

        while (connectionState.isTransfer()) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("num bytes available to read: %d",
                                        _in.numBytesAvailable()));
            }

            final int index = _in.decodeIndex();
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("Received index %d", index));
            }

            if (index == Filelist.DONE) {
                if (_fileSelection != FileSelection.RECURSE &&
                    !fileList.isEmpty())
                {
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
                if (_fileSelection != FileSelection.RECURSE) {
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
                if (_fileSelection != FileSelection.RECURSE) {
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
                        directoryIndex, directory.pathOrNull()));
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
                        "Error: received file index %d when listing files only",
                        index));
                }

                final char iFlags = _in.getChar();
                if (!Item.isValidItem(iFlags)) {
                    throw new IllegalStateException(String.format(
                            "got flags %s - not supported",
                            Integer.toBinaryString(iFlags)));
                }

                if ((iFlags & Item.TRANSFER) == 0) {
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine(String.format(
                                "index %d is not a transfer", index));
                    }
                    continue;
                }

                if (connectionState.isTearingDown()) {
                    throw new RsyncProtocolException(
                        String.format("Error: wrong phase (%s)",
                                      connectionState));
                }

                FileInfo fileInfo = segment.getFileWithIndexOrNull(index);
                if (fileInfo == null) {
                    if (_fileSelection != FileSelection.RECURSE) {
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
                    _log.info(fileInfo.pathOrNull().toString());
                }

                _stats.setNumTransferredFiles(_stats.numTransferredFiles() + 1);
                _stats.setTotalTransferredSize(_stats.totalTransferredSize() +
                                               fileInfo.attrs().size());

                if (isTransferred(index) && _log.isLoggable(Level.FINE)) {
                    _log.fine("Re-receiving " + fileInfo.pathOrNull());
                }
                receiveAndMatch(segment, index, fileInfo);
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
        _in.get(ByteBuffer.wrap(peerMd5sum));
        boolean isIdentical = tempSize == fileInfo.attrs().size() &&
                              Arrays.equals(md5sum, peerMd5sum);

        //isIdentical = isIdentical && Util.randomChance(0.25);

        if (_log.isLoggable(Level.FINE)) {
            if (isIdentical) {
                _log.fine(String.format("%s data received OK (remote and " +
                                        "local checksum is %s)",
                                        fileInfo.pathOrNull(),
                                        MD5.md5DigestToString(md5sum)));
            } else {
                _log.fine(String.format("%s checksum/size mismatch : " +
                                        "our=%s (size=%d), peer=%s (size=%d)",
                                        fileInfo.pathOrNull(),
                                        MD5.md5DigestToString(md5sum),
                                        tempSize,
                                        MD5.md5DigestToString(peerMd5sum),
                                        fileInfo.attrs().size()));
            }
        }
        return isIdentical;
    }

    private void moveTempfileToTarget(Path tempFile, Path target)
            throws InterruptedException
    {
        boolean isOK = FileOps.atomicMove(tempFile, target);
        if (!isOK) {
            String msg = String.format("Error: when moving temporary file %s " +
                                       "to %s", tempFile, target);
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe(msg);
            }
            _generator.sendMessage(MessageCode.ERROR_XFER, msg);
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
        if (_isPreserveTimes &&
            curAttrs.lastModifiedTime() != targetAttrs.lastModifiedTime())
        {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                    "updating mtime %d -> %d on %s",
                    curAttrs.lastModifiedTime(),
                    targetAttrs.lastModifiedTime(), path));
            }
            FileOps.setLastModifiedTime(path, targetAttrs.lastModifiedTime(),
                                        LinkOption.NOFOLLOW_LINKS);
        }
        if (_isPreserveUser) {
            if (!_isNumericIds && !targetAttrs.user().name().isEmpty() &&
                !curAttrs.user().name().equals(targetAttrs.user().name()))
            {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("updating ownership %s -> %s on %s",
                                            curAttrs.user(), targetAttrs.user(),
                                            path));
                }
                // FIXME: side effect of chown in Linux is that set user/group
                // id bit are cleared.
                FileOps.setOwner(path, targetAttrs.user(),
                                 LinkOption.NOFOLLOW_LINKS);
            } else if ((_isNumericIds || targetAttrs.user().name().isEmpty()) &&
                       curAttrs.user().id() != targetAttrs.user().id()) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                            "updating uid %d -> %d on %s",
                            curAttrs.user().id(), targetAttrs.user().id(),
                            path));
                }
                // NOTE: side effect of chown in Linux is that set user/group id
                // bit might be cleared.
                FileOps.setUserId(path, targetAttrs.user().id(),
                                  LinkOption.NOFOLLOW_LINKS);
            }
        }
        if (_isPreserveGroup) {
            if (!_isNumericIds && !targetAttrs.group().name().isEmpty() &&
                !curAttrs.group().name().equals(targetAttrs.group().name()))
            {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("updating group %s -> %s on %s",
                                            curAttrs.group(),
                                            targetAttrs.group(), path));
                }
                FileOps.setGroup(path, targetAttrs.group(),
                                 LinkOption.NOFOLLOW_LINKS);
            } else if ((_isNumericIds ||
                        targetAttrs.group().name().isEmpty()) &&
                       curAttrs.group().id() != targetAttrs.group().id()) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("updating gid %d -> %d on %s",
                                            curAttrs.group().id(),
                                            targetAttrs.group().id(), path));
                }
                FileOps.setGroupId(path, targetAttrs.group().id(),
                                   LinkOption.NOFOLLOW_LINKS);
            }
        }
    }

    private void receiveAndMatch(Filelist.Segment segment, int index,
                                 FileInfo fileInfo)
        throws ChannelException, InterruptedException
    {
        Checksum.Header checksumHeader = receiveChecksumHeader();
        if (_log.isLoggable(Level.FINE)) {
            _log.fine("received peer checksum " + checksumHeader);
        }

        try (AutoDeletable tempFile = new AutoDeletable(
                Files.createTempFile(fileInfo.pathOrNull().getParent(),
                                     null, null)))
        {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("created tempfile " + tempFile);
            }
            matchData(segment, index, fileInfo, checksumHeader,
                      tempFile.path());
        } catch (IOException e) {
            String msg = String.format("failed to create tempfile in %s: %s",
                                       fileInfo.pathOrNull().getParent(),
                                       e.getMessage());
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe(msg);
            }
            _generator.sendMessage(MessageCode.ERROR_XFER, msg + '\n');
            discardData(checksumHeader);
            _in.skip(Checksum.MAX_DIGEST_LENGTH);
            _ioError |= IoError.GENERAL;
            _generator.purgeFile(segment, index);
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
                if (_isPreservePermissions || _isPreserveTimes ||
                    _isPreserveUser || _isPreserveGroup)
                {
                    updateAttrsIfDiffer(resultFile, fileInfo.attrs());
                }
                if (!_isDeferWrite ||
                    !resultFile.equals(fileInfo.pathOrNull()))
                {
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine(String.format("moving %s -> %s",
                                                resultFile,
                                                fileInfo.pathOrNull()));
                    }
                    moveTempfileToTarget(resultFile, fileInfo.pathOrNull());
                }
            } catch (IOException e) {
                _ioError |= IoError.GENERAL;
                if (_log.isLoggable(Level.SEVERE)) {
                    _log.severe(String.format("failed to update attrs on %s: " +
                                              "%s",
                                              resultFile, e.getMessage()));
                }
            }
            _generator.purgeFile(segment, index);
        } else {
            if (isTransferred(index)) {
                _ioError |= IoError.GENERAL;
                try {
                    _generator.sendMessage(MessageCode.ERROR_XFER,
                                           String.format(
                                                   "%s (index %d) failed " +
                                                   "verification, update " +
                                                   "discarded\n",
                                                   fileInfo.pathOrNull(),
                                                   index));
                } catch (TextConversionException e) {
                    if (_log.isLoggable(Level.SEVERE)) {
                        _log.log(Level.SEVERE, "", e);
                    }
                }
                _generator.purgeFile(segment, index);
            } else {
                _generator.generateFile(segment, index, fileInfo);
                setIsTransferred(index);
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
            return IntegerCoder.decodeLong(_in, minBytes);
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
        long numBytesRead = _in.numBytesRead() -
                            _in.numBytesPrefetched();

        while (true) {
            char flags = (char) (_in.getByte() & 0xFF);
            if (flags == 0) {
                break;
            }
            if ((flags & TransmitFlags.EXTENDED_FLAGS) != 0) {
                flags |= (_in.getByte() & 0xFF) << 8;
                if (flags == (TransmitFlags.EXTENDED_FLAGS |
                              TransmitFlags.IO_ERROR_ENDLIST)) {
                    if (!_isSafeFileList) {
                        throw new RsyncProtocolException(
                               "invalid flag " + Integer.toBinaryString(flags));
                    }
                    ioError |= receiveAndDecodeInt();
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(String.format("peer process returned an " +
                                                   "I/O error (%d)", ioError));
                    }
                    _generator.disableDelete();
                    break;
                }
            }
            if (_log.isLoggable(Level.FINER)) {
                _log.finer("got flags " + Integer.toBinaryString(flags));
            }
            byte[] pathNameBytes = receivePathNameBytes(flags);
            RsyncFileAttributes attrs = receiveRsyncFileAttributes(flags);
            String pathName = _characterDecoder.decodeOrNull(pathNameBytes);
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("Receiving file information for %s: %s",
                                        pathName, attrs));
            }

            FileInfoStub stub = new FileInfoStub(pathName, pathNameBytes,
                                                 attrs);

            if ((_isPreserveDevices &&
                 (attrs.isBlockDevice() || attrs.isCharacterDevice())) ||
                (_isPreserveSpecials &&
                 (attrs.isFifo() || attrs.isSocket())))
            {
                if ((flags & TransmitFlags.SAME_RDEV_MAJOR) == 0) {
                    stub._major = receiveAndDecodeInt();
                }
                stub._minor = receiveAndDecodeInt();
            } else if (_isPreserveLinks && attrs.isSymbolicLink()) {
                try {
                    String symlinkTarget = receiveSymlinkTarget();
                    stub._symlinkTargetOrNull = Paths.get(symlinkTarget);
                } catch (InvalidPathException e) {
                    throw new RsyncProtocolException(e);
                }
            }

            builder.add(stub);
        }

        long segmentSize = _in.numBytesRead() -
                           _in.numBytesPrefetched() - numBytesRead;
        _stats.setTotalFileListSize(_stats.totalFileListSize() + segmentSize);
        return ioError;
    }

    private String receiveSymlinkTarget() throws ChannelException
    {
        int length = receiveAndDecodeInt();
        ByteBuffer buf = _in.get(length);
        String name = _characterDecoder.decode(buf);
        return name;
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

            if (_log.isLoggable(Level.FINER)) {
                _log.finer(String.format("Extracting FileInfo stub: " +
                                         "pathName=%s %s", pathName, attrs));
            }

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
            } else if (!PathOps.isDirectoryStructurePreservable(pathName)) {
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
                    if (_log.isLoggable(Level.FINER)) {
                        _log.finer(String.format(
                                "relative path: %s, full path: %s",
                                relativePath, fullPath));
                    }
                    if (PathOps.isPathPreservable(fullPath)) {
                        // throws IllegalArgumentException but this is avoided
                        // due to previous checks
                        if ((attrs.isBlockDevice() ||
                             attrs.isCharacterDevice() ||
                             attrs.isFifo() ||
                             attrs.isSocket()) &&
                            stub._major >= 0 && stub._minor >= 0)
                        {
                            fileInfo = new DeviceInfo(fullPath, relativePath,
                                                      pathNameBytes, attrs,
                                                      stub._major, stub._minor);
                        } else if (attrs.isSymbolicLink() &&
                                   stub._symlinkTargetOrNull != null) {
                            fileInfo = new SymlinkInfo(fullPath,
                                                       relativePath,
                                                       pathNameBytes,
                                                       attrs,
                                                       stub._symlinkTargetOrNull);
                        } else {
                            fileInfo = new FileInfo(fullPath,
                                                    relativePath,
                                                    pathNameBytes,
                                                    attrs);
                        }
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
            if (fileInfo == null) {
                fileInfo = FileInfo.newUntransferrable(pathNameBytes, attrs);
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
            prefixNumBytes = 0xFF & _in.getByte();
        }
        int suffixNumBytes;
        if ((xflags & TransmitFlags.LONG_NAME) != 0) {
            suffixNumBytes = receiveAndDecodeInt();
        } else {
            suffixNumBytes = 0xFF & _in.getByte();
        }

        byte[] prevFileNameBytes = _fileInfoCache.getPrevFileNameBytes();
        byte[] fileNameBytes = new byte[prefixNumBytes + suffixNumBytes];
        Util.copyArrays(prevFileNameBytes, fileNameBytes, prefixNumBytes);
        _in.get(fileNameBytes, prefixNumBytes, suffixNumBytes);
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
            mode = _in.getInt();
            _fileInfoCache.setPrevMode(mode);
        }

        User user;
        boolean reusePrevUserId = (xflags & TransmitFlags.SAME_UID) != 0;
        if (reusePrevUserId) {
            user = getPreviousUser();
        } else {
            if (!_isPreserveUser) {
                throw new RsyncProtocolException("got new uid when not " +
                                                 "preserving uid");
            }
            boolean isReceiveUserName =
                (xflags & TransmitFlags.USER_NAME_FOLLOWS) != 0;
            if (isReceiveUserName && _fileSelection != FileSelection.RECURSE) {
                throw new RsyncProtocolException("got user name mapping when " +
                                                 "not doing incremental " +
                                                 "recursion");
            } else if (isReceiveUserName && _isNumericIds) {
                throw new RsyncProtocolException("got user name mapping with " +
                                                 "--numeric-ids");
            }
            if (_fileSelection == FileSelection.RECURSE && isReceiveUserName) {
                user = receiveUser();
                _uidUserMap.put(user.id(), user);
            } else if (_fileSelection == FileSelection.RECURSE) {
                // && !isReceiveUsername where isReceiveUserName is only true
                // once for every new mapping, old ones have been sent
                // previously
                int uid = receiveUserId();
                // Note: _uidUserMap contains a predefined mapping for root
                user = _uidUserMap.get(uid);
                if (user == null) {
                    user = new User("", uid);
                }
            } else { // if (_fileSelection != FileSelection.RECURSE) {
                // User with uid but no user name. User name mappings are sent
                // in batch after initial file list
                user = receiveIncompleteUser();
            }
            _fileInfoCache.setPrevUser(user);
        }

        Group group;
        boolean reusePrevGroupId = (xflags & TransmitFlags.SAME_GID) != 0;
        if (reusePrevGroupId) {
            group = getPreviousGroup();
        } else {
            if (!_isPreserveGroup) {
                throw new RsyncProtocolException("got new gid when not " +
                                                 "preserving gid");
            }
            boolean isReceiveGroupName =
                (xflags & TransmitFlags.GROUP_NAME_FOLLOWS) != 0;
            if (isReceiveGroupName && _fileSelection != FileSelection.RECURSE) {
                throw new RsyncProtocolException("got group name mapping " +
                                                 "when not doing incremental " +
                                                 "recursion");
            } else if (isReceiveGroupName && _isNumericIds) {
                throw new RsyncProtocolException("got group name mapping " +
                                                 "with --numeric-ids");
            }
            if (_fileSelection == FileSelection.RECURSE && isReceiveGroupName) {
                group = receiveGroup();
                _gidGroupMap.put(group.id(), group);
            } else if (_fileSelection == FileSelection.RECURSE) {
                int gid = receiveGroupId();
                group = _gidGroupMap.get(gid);
                if (group == null) {
                    group = new Group("", gid);
                }
            } else { // if (_fileSelection != FileSelection.RECURSE) {
                // Group with gid but no group name. Group name mappings are
                // sent in batch after initial file list
                group = receiveIncompleteGroup();
            }
            _fileInfoCache.setPrevGroup(group);
        }

        // throws IllegalArgumentException if fileSize or lastModified is
        // negative, but we check for this earlier
        RsyncFileAttributes attrs = new RsyncFileAttributes(mode,
                                                            fileSize,
                                                            lastModified,
                                                            user,
                                                            group);
        return attrs;
    }

    private User getPreviousUser()
    {
        User user = _fileInfoCache.getPrevUserOrNull();
        if (user == null) {
            if (_isPreserveUser) {
                throw new RsyncProtocolException("expecting to receive user " +
                                                 "information from peer");
            }
            return User.JVM_USER;
        }
        return user;
    }

    private Group getPreviousGroup()
    {
        Group group = _fileInfoCache.getPrevGroupOrNull();
        if (group == null) {
            if (_isPreserveGroup) {
                throw new RsyncProtocolException("expecting to receive group " +
                                                 "information from peer");
            }
            return Group.JVM_GROUP;
        }
        return group;
    }

    private User receiveIncompleteUser() throws ChannelException
    {
        int uid = receiveUserId();
        return new User("", uid);
    }

    private Group receiveIncompleteGroup() throws ChannelException
    {
        int gid = receiveGroupId();
        return new Group("", gid);
    }

    private int receiveUserId() throws ChannelException
    {
        int uid = receiveAndDecodeInt();
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("received user id " + uid);
        }
        if (uid < 0 || uid > AbstractPrincipal.ID_MAX) {
            throw new RsyncProtocolException(String.format(
                "received illegal value for user id: %d (valid range [0..%d]",
                uid, AbstractPrincipal.ID_MAX));
        }
        return uid;
    }

    private int receiveGroupId() throws ChannelException
    {
        int gid = receiveAndDecodeInt();
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("received group id " + gid);
        }
        if (gid < 0 || gid > AbstractPrincipal.ID_MAX) {
            throw new RsyncProtocolException(String.format(
                "received illegal value for group id: %d (valid range [0..%d]",
                gid, AbstractPrincipal.ID_MAX));
        }
        return gid;
    }

    /**
     * @throws RsyncProtocolException if user name is the empty string
     */
    private String receiveUserName() throws ChannelException
    {
        int nameLength = 0xFF & _in.getByte();
        ByteBuffer buf = _in.get(nameLength);
        String userName = _characterDecoder.decode(buf);
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("received user name " + userName);
        }
        if (userName.isEmpty()) {
            throw new RsyncProtocolException("user name is empty");
        }
        return userName;
    }

    /**
     * @throws RsyncProtocolException if user name is the empty string
     */
    private String receiveGroupName() throws ChannelException
    {
        int nameLength = 0xFF & _in.getByte();
        ByteBuffer buf = _in.get(nameLength);
        String groupName = _characterDecoder.decode(buf);
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("received group name " + groupName);
        }
        if (groupName.isEmpty()) {
            throw new RsyncProtocolException("group name is empty");
        }
        return groupName;
    }

    private User receiveUser() throws ChannelException
    {
        int uid = receiveUserId();
        String userName = receiveUserName();
        return new User(userName, uid);
    }

    private Group receiveGroup() throws ChannelException
    {
        int gid = receiveGroupId();
        String groupName = receiveGroupName();
        return new Group(groupName, gid);
    }

    private void discardData(Checksum.Header checksumHeader)
        throws ChannelException
    {
        long sizeLiteral = 0;
        long sizeMatch = 0;
        while (true) {
            int token = _in.getInt();
            if (token == 0) {
                break;
            } else if (token > 0) {
                int numBytes = token;
                _in.skip(numBytes);
                sizeLiteral += numBytes;
            } else {
                // blockIndex >= 0 && blockIndex <= Integer.MAX_VALUE
                final int blockIndex = - (token + 1);
                sizeMatch += blockSize(blockIndex, checksumHeader);
            }
        }
        _stats.setTotalLiteralSize(_stats.totalLiteralSize() + sizeLiteral);
        _stats.setTotalMatchedSize(_stats.totalMatchedSize() + sizeMatch);
    }

    private Path mergeDataFromPeerAndReplica(FileInfo fileInfo,
                                             Path tempFile,
                                             Checksum.Header checksumHeader,
                                             MessageDigest md)
            throws ChannelException, InterruptedException
    {
        assert fileInfo != null;
        assert tempFile != null;
        assert checksumHeader != null;
        assert md != null;

        try (FileChannel target = FileChannel.open(tempFile,
                                                   StandardOpenOption.WRITE)) {
            Path p = fileInfo.pathOrNull();
            try (FileChannel replica =
                    FileChannel.open(p, StandardOpenOption.READ)) {
                RsyncFileAttributes attrs = RsyncFileAttributes.stat(p);
                if (attrs.isRegularFile()) {
                    boolean isIntact = combineDataToFile(replica, target,
                                                         checksumHeader, md);
                    if (isIntact) {
                        RsyncFileAttributes attrs2 =
                                RsyncFileAttributes.statOrNull(p);
                        if (!attrs.equals(attrs2)) {
                            String msg = String.format(
                                    "%s modified during verification " +
                                    "(%s != %s)", p, attrs, attrs2);
                            if (_log.isLoggable(Level.WARNING)) {
                                _log.warning(msg);
                            }
                            _generator.sendMessage(MessageCode.WARNING, msg);
                            md.update((byte) 0);
                        }
                        return p;
                    }
                    return tempFile;
                } // else discard later
            } catch (NoSuchFileException e) {  // replica.open
                combineDataToFile(null, target, checksumHeader, md);
                return tempFile;
            }
        } catch (IOException e) {        // target.open
            // discard below
        }
        discardData(checksumHeader);
        return null;
    }

    private boolean combineDataToFile(FileChannel replicaOrNull,
                                      FileChannel target,
                                      Checksum.Header checksumHeader,
                                      MessageDigest md)
        throws IOException, ChannelException
    {
        assert target != null;
        assert checksumHeader != null;
        assert md != null;

        boolean isDeferrable = _isDeferWrite && replicaOrNull != null;
        long sizeLiteral = 0;
        long sizeMatch = 0;
        int expectedIndex = 0;

        while (true) {
            final int token = _in.getInt();
            if (token == 0) {
                break;
            }
            // token correlates to a matching block index
            if (token < 0) {
                // blockIndex >= 0 && blockIndex <= Integer.MAX_VALUE
                final int blockIndex = - (token + 1);
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
                        "blockLength = %d)",
                        blockIndex, checksumHeader.blockLength()));
                } else if (replicaOrNull == null) {
                    // or we could alternatively read zeroes from replica and
                    // have the correct file size in the end?
                    //
                    // i.e. generator sent file info to sender and sender
                    // replies with a match but now our replica is gone
                    continue;
                }

                sizeMatch += blockSize(blockIndex, checksumHeader);

                if (isDeferrable) {
                    if (blockIndex == expectedIndex) {
                        expectedIndex++;
                        continue;
                    }
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine(String.format("defer write disabled since " +
                                                "%d != %d",
                                                blockIndex, expectedIndex));
                    }
                    isDeferrable = false;
                    for (int i = 0; i < expectedIndex; i++) {
                        copyFromReplicaAndUpdateDigest(replicaOrNull, i,
                                                       target, md,
                                                       checksumHeader);
                    }
                }
                copyFromReplicaAndUpdateDigest(replicaOrNull, blockIndex,
                                               target, md, checksumHeader);
            } else if (token > 0) { // receive literal data from peer:
                if (isDeferrable) {
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine(String.format("defer write disabled since " +
                                                "we got literal data %d",
                                                token));
                    }
                    isDeferrable = false;
                    for (int i = 0; i < expectedIndex; i++) {
                        copyFromReplicaAndUpdateDigest(replicaOrNull, i,
                                                       target, md,
                                                       checksumHeader);
                    }
                }
                int length = token;
                sizeLiteral += length;
                copyFromPeerAndUpdateDigest(target, length, md);
            }
        }

        // rare truncation of multiples of checksum blocks
        if (isDeferrable && expectedIndex != checksumHeader.chunkCount()) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("defer write disabled since " +
                                        "expectedIndex %d != " +
                                        "checksumHeader.chunkCount() %d",
                                        expectedIndex,
                                        checksumHeader.chunkCount()));
            }
            isDeferrable = false;
            for (int i = 0; i < expectedIndex; i++) {
                copyFromReplicaAndUpdateDigest(replicaOrNull, i,  target, md,
                                               checksumHeader);
            }
        }
        if (isDeferrable) {
            // expectedIndex == checksumHeader.chunkCount()
            for (int i = 0; i < expectedIndex; i++) {
                ByteBuffer replicaBuf = readFromReplica(replicaOrNull, i,
                                                        checksumHeader);
                md.update(replicaBuf);
            }
        }

        if (_log.isLoggable(Level.FINE)) {
            if (_isDeferWrite && replicaOrNull != null && !isDeferrable) {
                _log.fine("defer write disabled");
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
        return isDeferrable;
    }

    private void copyFromPeerAndUpdateDigest(FileChannel target, int length,
                                             MessageDigest md)
        throws ChannelException
    {
        int bytesReceived = 0;
        while (bytesReceived < length) {
            int chunkSize = Math.min(INPUT_CHANNEL_BUF_SIZE,
                                     length - bytesReceived);
            ByteBuffer literalData = _in.get(chunkSize);
            bytesReceived += chunkSize;
            literalData.mark();
            writeToFile(target, literalData);
            literalData.reset();
            md.update(literalData);
        }
    }

    private void copyFromReplicaAndUpdateDigest(FileChannel replica,
                                                int blockIndex,
                                                FileChannel target,
                                                MessageDigest md,
                                                Checksum.Header checksumHeader)
        throws IOException
    {
        ByteBuffer replicaBuf = readFromReplica(replica, blockIndex,
                                                checksumHeader);
        writeToFile(target, replicaBuf);
        // it's OK to rewind since the buffer is newly allocated with an initial
        // position of zero
        replicaBuf.rewind();
        md.update(replicaBuf);
    }

    private ByteBuffer readFromReplica(FileChannel replica,
                                       int blockIndex,
                                       Checksum.Header checksumHeader)
            throws IOException
    {
        int length = blockSize(blockIndex, checksumHeader);
        long offset = (long) blockIndex * checksumHeader.blockLength();
        return readFromReplica(replica, offset, length);
    }

    private ByteBuffer readFromReplica(FileChannel replica, long offset,
                                       int length)
            throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate(length);
        int bytesRead = replica.read(buf, offset);
        if (buf.hasRemaining()) {
            throw new IllegalStateException(String.format(
                "truncated read from replica (%s), read %d bytes but expected" +
                " %d more bytes", replica, bytesRead, buf.remaining()));
        }
        buf.flip();
        return buf;
    }

    private static int blockSize(int index, Checksum.Header checksumHeader)
    {
        if (index == checksumHeader.chunkCount() - 1 &&
            checksumHeader.remainder() != 0) {
            return checksumHeader.remainder();
        }
        return checksumHeader.blockLength();
    }

    private void writeToFile(FileChannel out, ByteBuffer src)
    {
        try {
            // NOTE: might notably fail due to running out of disk space
            out.write(src);
            if (src.hasRemaining()) {
                throw new IllegalStateException(String.format(
                    "truncated write to %s, returned %d bytes, " +
                    "expected %d more bytes",
                    out, src.position(), src.remaining()));
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
            // dummy read to get any final messages from peer
            byte dummy = _in.getByte();
            // we're not expected to get this far, getByte should throw
            // NetworkEOFException
            throw new RsyncProtocolException(
                String.format("Peer sent invalid data during connection tear " +
                              "down (%d)", dummy));
        } catch (ChannelEOFException e) {
            // It's OK, we expect EOF without having received any data
        }
    }

    private void setIsTransferred(int index)
    {
        _transferred.set(index);
    }

    private boolean isTransferred(int index)
    {
        return _transferred.get(index);
    }
}
