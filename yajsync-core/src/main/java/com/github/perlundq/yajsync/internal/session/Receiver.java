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
package com.github.perlundq.yajsync.internal.session;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.FileSelection;
import com.github.perlundq.yajsync.RsyncException;
import com.github.perlundq.yajsync.RsyncProtocolException;
import com.github.perlundq.yajsync.RsyncSecurityException;
import com.github.perlundq.yajsync.Statistics;
import com.github.perlundq.yajsync.attr.FileInfo;
import com.github.perlundq.yajsync.attr.Group;
import com.github.perlundq.yajsync.attr.LocatableFileInfo;
import com.github.perlundq.yajsync.attr.RsyncFileAttributes;
import com.github.perlundq.yajsync.attr.User;
import com.github.perlundq.yajsync.internal.channels.ChannelEOFException;
import com.github.perlundq.yajsync.internal.channels.ChannelException;
import com.github.perlundq.yajsync.internal.channels.Message;
import com.github.perlundq.yajsync.internal.channels.MessageCode;
import com.github.perlundq.yajsync.internal.channels.MessageHandler;
import com.github.perlundq.yajsync.internal.channels.RsyncInChannel;
import com.github.perlundq.yajsync.internal.io.AutoDeletable;
import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.text.TextConversionException;
import com.github.perlundq.yajsync.internal.text.TextDecoder;
import com.github.perlundq.yajsync.internal.util.Environment;
import com.github.perlundq.yajsync.internal.util.FileOps;
import com.github.perlundq.yajsync.internal.util.MD5;
import com.github.perlundq.yajsync.internal.util.PathOps;
import com.github.perlundq.yajsync.internal.util.RuntimeInterruptException;
import com.github.perlundq.yajsync.internal.util.Util;

public class Receiver implements RsyncTask, MessageHandler
{
    public static class Builder
    {
        private final Generator _generator;
        private final ReadableByteChannel _in;
        private final Path _targetPath;
        private boolean _isDeferWrite;
        private boolean _isExitAfterEOF;
        private boolean _isExitEarlyIfEmptyList;
        private boolean _isReceiveStatistics;
        private boolean _isSafeFileList = true;
        private FilterMode _filterMode = FilterMode.NONE;

        public User _defaultUser = User.NOBODY;
        public Group _defaultGroup = Group.NOBODY;
        public int _defaultFilePermissions = Environment.DEFAULT_FILE_PERMS;
        public int _defaultDirectoryPermissions = Environment.DEFAULT_DIR_PERMS;

        public Builder(Generator generator, ReadableByteChannel in,
                       Path targetPath)
        {
            assert generator != null;
            assert in != null;
            assert targetPath == null || targetPath.isAbsolute();
            _generator = generator;
            _in = in;
            _targetPath = targetPath;
        }

        public static Builder newListing(Generator generator,
                                         ReadableByteChannel in)
        {
            return new Builder(generator, in, null);
        }

        public static Builder newServer(Generator generator,
                                        ReadableByteChannel in,
                                        Path targetPath)
        {
            return new Builder(generator, in, targetPath).
                    isReceiveStatistics(false).
                    isExitEarlyIfEmptyList(false).
                    isExitAfterEOF(false);
        }

        public static Builder newClient(Generator generator,
                                        ReadableByteChannel in,
                                        Path targetPath)
        {
            return new Builder(generator, in, targetPath).
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

        public Builder defaultUser(User defaultUser)
        {
            _defaultUser = defaultUser;
            return this;
        }

        public Builder defaultGroup(Group defaultGroup)
        {
            _defaultGroup = defaultGroup;
            return this;
        }

        public Builder defaultFilePermissions(int defaultFilePermissions)
        {
            _defaultFilePermissions = defaultFilePermissions;
            return this;
        }

        public Builder defaultDirectoryPermissions(int defaultDirectoryPermissions)
        {
            _defaultDirectoryPermissions = defaultDirectoryPermissions;
            return this;
        }

        public Receiver build()
        {
            return new Receiver(this);
        }
    }

    private static class FileInfoStub {
        private String _pathNameOrNull;
        private byte[] _pathNameBytes;
        private RsyncFileAttributes _attrs;
        private String _symlinkTargetOrNull;
        private int _major = -1;
        private int _minor = -1;

        @Override
        public String toString() {
            return String.format("%s(%s, %s)", getClass().getSimpleName(),
                                 _pathNameOrNull, _attrs);
        }
    }

    private interface PathResolver
    {
        /**
         * @throws InvalidPathException
         * @throws RsyncSecurityException
         */
        Path relativePathOf(String pathName) throws RsyncSecurityException;

        /**
         * @throws RsyncSecurityException
         */
        Path fullPathOf(Path relativePath) throws RsyncSecurityException;
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
    private final FileAttributeManager _fileAttributeManager;
    private final FileInfoCache _fileInfoCache = new FileInfoCache();
    private final Filelist _fileList;
    private final FileSelection _fileSelection;
    private final FilterMode _filterMode;
    private final Generator _generator;
    private final Map<Integer, User> _recursiveUidUserMap = new HashMap<>();
    private final Map<Integer, Group> _recursiveGidGroupMap = new HashMap<>();
    private final RsyncInChannel _in;
    private final SessionStatistics _stats = new SessionStatistics();
    private final Path _targetPath; // is null if file listing
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
        _fileList = _generator.fileList();
        _isInterruptible = _generator.isInterruptible();
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
        _targetPath = builder._targetPath;
        _isListOnly = _targetPath == null;
        _characterDecoder = TextDecoder.newStrict(_generator.charset());

        if (!_isListOnly) {
            _fileAttributeManager = FileAttributeManagerFactory.getMostPerformant(
                                                              _targetPath.getFileSystem(),
                                                              _isPreserveUser,
                                                              _isPreserveGroup,
                                                              _isPreserveDevices,
                                                              _isPreserveSpecials,
                                                              _isNumericIds,
                                                              builder._defaultUser,
                                                              builder._defaultGroup,
                                                              builder._defaultFilePermissions,
                                                              builder._defaultDirectoryPermissions);
            _generator.setFileAttributeManager(_fileAttributeManager);
        } else {
            _fileAttributeManager = null;
        }
    }

    @Override
    public String toString()
    {
        return String.format(
                "%s(" +
                "isDeferWrite=%b, " +
                "isExitAfterEOF=%b, " +
                "isExitEarlyIfEmptyList=%b, " +
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
                "isReceiveStatistics=%b, " +
                "isSafeFileList=%b, " +
                "fileSelection=%s, " +
                "filterMode=%s, " +
                "targetPath=%s, " +
                "fileAttributeManager=%s" +
                ")",
                getClass().getSimpleName(),
                _isDeferWrite,
                _isExitAfterEOF,
                _isExitEarlyIfEmptyList,
                _isListOnly,
                _isInterruptible,
                _isNumericIds,
                _isPreserveDevices,
                _isPreserveLinks,
                _isPreservePermissions,
                _isPreserveSpecials,
                _isPreserveTimes,
                _isPreserveUser,
                _isPreserveGroup,
                _isReceiveStatistics,
                _isSafeFileList,
                _fileSelection,
                _filterMode,
                _targetPath,
                _fileAttributeManager);
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

    /**
     * @throws ChannelException if there is a communication failure with peer
     * @throws RsyncProtocolException if peer does not conform to the rsync
     *     protocol
     * @throws RsyncSecurityException if peer misbehaves such that the integrity
     *     of the application is impacted
     * @throws RsyncException if:
     *     1. failing to stat target (it is OK if it does not exist though)
     *     2. target does not exist and failing to create missing directories
     *        to it
     *     3. target exists but is neither a directory nor a file nor a
     *        symbolic link (at least until there is proper support for
     *        additional file types)
     *     4. target is an existing file and there are two or more source
     *        files
     *     5. target is an existing file and the first source argument is an
     *        existing directory
     */
    @Override
    public Boolean call() throws RsyncException, InterruptedException
    {
        try {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(this.toString());
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
                    _recursiveUidUserMap.put(User.ROOT.id(), User.ROOT);
                }
                if (_isPreserveGroup) {
                    _recursiveGidGroupMap.put(Group.ROOT.id(), Group.ROOT);
                }
            }

            Filelist.Segment initialSegment = receiveInitialSegment();

            if (initialSegment.isFinished() && _isExitEarlyIfEmptyList) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("empty file list %s - exiting " +
                                            "early", initialSegment));
                }
                if (_fileSelection == FileSelection.RECURSE) {
                    int dummyIndex = _in.decodeIndex();
                    if (dummyIndex != Filelist.EOF) {
                        throw new RsyncProtocolException(String.format(
                                "expected peer to send index %d (EOF), but " +
                                "got %d", Filelist.EOF, dummyIndex));
                    }
                }
                // NOTE: we never _receive_ any statistics if initial file list
                // is empty
                if (_isExitAfterEOF) {
                    readAllMessagesUntilEOF();
                }
                return _ioError == 0;
            }

            if (_isListOnly) {
                _generator.listSegment(initialSegment);
            } else {
                _generator.generateSegment(initialSegment);
            }
            _ioError |= receiveFiles();
            _stats._numFiles = _fileList.numFiles();
            if (_isReceiveStatistics) {
                receiveStatistics();
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                        "(local) Total file size: %d bytes, Total bytes sent:" +
                        " %d, Total bytes received: %d",
                        _fileList.totalFileSize(),
                        _generator.numBytesWritten(),
                        _in.numBytesRead()));
                }
            }

            if (_isExitAfterEOF) {
                readAllMessagesUntilEOF();
            }
            return _ioError == 0;
        } catch (RuntimeInterruptException e) {
            throw new InterruptedException();
        } finally {
            _generator.stop();
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("exit status %d", _ioError));
            }
        }
    }

    /**
     * @throws RsyncProtocolException if failing to decode the filter rules
     */
    private String receiveFilterRules() throws ChannelException,
                                               RsyncProtocolException
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
    private Map<Integer, User> receiveUserList() throws ChannelException,
                                                        RsyncProtocolException
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
    private Map<Integer, Group> receiveGroupList() throws ChannelException,
                                                          RsyncProtocolException
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
            throws RsyncProtocolException
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
            throws RsyncProtocolException
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
     *
     * @throws RsyncException if:
     *     1. failing to stat target (it is OK if it does not exist though)
     *     2. if target does not exist and failing to create missing directories
     *        to it,
     *     3. if target exists but is neither a directory nor a file nor a
     *        symbolic link (at least until there is proper support for
     *        additional file types).
     *     4. if target is an existing file and there are two or more source
     *        files.
     *     5. if target is an existing file and the first source argument is an
     *        existing directory.
     */
    private PathResolver getPathResolver(final List<FileInfoStub> stubs)
        throws RsyncException
    {
        RsyncFileAttributes attrs;
        try {
            attrs = _fileAttributeManager.statIfExists(_targetPath);
        } catch (IOException e) {
            throw new RsyncException(String.format("unable to stat %s: %s",
                                                   _targetPath, e));
        }

        boolean isTargetExisting = attrs != null;
        boolean isTargetExistingDir =
            isTargetExisting && attrs.isDirectory();
        boolean isTargetExistingFile =
            isTargetExisting && !attrs.isDirectory();
        boolean isSourceSingleFile =
            stubs.size() == 1 && !stubs.get(0)._attrs.isDirectory();
        boolean isTargetNonExistingFile = !isTargetExisting &&
                                          !_targetPath.endsWith(Text.DOT);

        if (_log.isLoggable(Level.FINER)) {
            _log.finer(String.format(
                    "targetPath=%s attrs=%s isTargetExisting=%s " +
                    "isSourceSingleFile=%s " +
                    "isTargetNonExistingFile=%s " +
                    "#stubs=%d",
                    _targetPath, attrs, isTargetExisting,
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
                    FileSystem fs = _targetPath.getFileSystem();
                    return fs.getPath(stubs.get(0)._pathNameOrNull);
                }
                @Override public Path fullPathOf(Path relativePath) {
                    return _targetPath;
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
                               _targetPath);
                }
                try {
                    Files.createDirectories(_targetPath);
                } catch (IOException e) {
                    throw new RsyncException(String.format(
                            "unable to create directory structure to %s: %s",
                            _targetPath, e));
                }
            }
            return new PathResolver() {
                @Override public Path relativePathOf(String pathName)
                        throws RsyncSecurityException {
                    // throws InvalidPathException
                    FileSystem fs = _targetPath.getFileSystem();
                    Path relativePath = fs.getPath(pathName);
                    if (relativePath.isAbsolute()) {
                        throw new RsyncSecurityException(relativePath +
                            " is absolute");
                    }
                    Path normalizedRelativePath =
                        PathOps.normalizeStrict(relativePath);
                    return normalizedRelativePath;
                }
                @Override public Path fullPathOf(Path relativePath) throws RsyncSecurityException {
                    Path fullPath =
                        _targetPath.resolve(relativePath).normalize();
                    if (!fullPath.startsWith(_targetPath.normalize())) {
                        throw new RsyncSecurityException(String.format(
                            "%s is outside of receiver destination dir %s",
                            fullPath, _targetPath));
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
            throw new RsyncException(String.format(
                "refusing to overwrite existing target path %s which is " +
                "neither a file nor a directory (%s)", _targetPath, attrs));
        }
        if (isTargetExistingFile && stubs.size() >= 2) {
            throw new RsyncException(String.format(
                "refusing to copy source files %s into file %s " +
                "(%s)", stubs, _targetPath, attrs));
        }
        if (isTargetExistingFile && stubs.size() == 1 &&
            stubs.get(0)._attrs.isDirectory()) {
            throw new RsyncException(String.format(
                "refusing to recursively copy directory %s into " +
                "non-directory %s (%s)", stubs.get(0), _targetPath, attrs));
        }
        throw new AssertionError(String.format(
            "BUG: stubs=%s targetPath=%s attrs=%s",
            stubs, _targetPath, attrs));
    }

    private Filelist.Segment receiveInitialSegment()
            throws InterruptedException, RsyncException
    {
        // unable to resolve path until we have the initial list of files
        List<FileInfoStub> stubs = receiveFileStubs();
        if (!_isListOnly) {
            _pathResolver = getPathResolver(stubs);
            if (_log.isLoggable(Level.FINER)) {
                _log.finer("Path Resolver: " + _pathResolver);
            }
        }

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

        Filelist.SegmentBuilder builder = new Filelist.SegmentBuilder(null);

        for (FileInfoStub stub : stubs) {
            Path pathOrNull = resolvePathOrNull(stub._pathNameOrNull);
            FileInfo f = createFileInfo(stub._pathNameOrNull,
                                        stub._pathNameBytes,
                                        stub._attrs,
                                        pathOrNull,
                                        stub._symlinkTargetOrNull,
                                        stub._major, stub._minor);
            builder.add(f);
        }

        Filelist.Segment segment = _fileList.newSegment(builder);
        return segment;
    }

    private void receiveStatistics() throws ChannelException
    {
        _stats._totalBytesWritten = receiveAndDecodeLong(3);
        _stats._totalBytesRead = receiveAndDecodeLong(3);
        _stats._totalFileSize = receiveAndDecodeLong(3);
        _stats._fileListBuildTime =  receiveAndDecodeLong(3);
        _stats._fileListTransferTime =  receiveAndDecodeLong(3);
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
    public void handleMessage(Message message) throws RsyncProtocolException
    {
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("got message " + message);
        }
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
    private void printMessage(Message message) throws RsyncProtocolException
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

    private void handleMessageNoSend(int index) throws RsyncProtocolException
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

    private Checksum.Header receiveChecksumHeader()
            throws ChannelException, RsyncProtocolException
    {
        return Connection.receiveChecksumHeader(_in);
    }

    private int receiveFiles() throws ChannelException, InterruptedException,
                                      RsyncProtocolException,
                                      RsyncSecurityException
    {
        int ioError = 0;
        Filelist.Segment segment = null;
        int numSegmentsInProgress = 1;
        TransferPhase phase = TransferPhase.TRANSFER;
        boolean isEOF = _fileSelection != FileSelection.RECURSE;

        while (phase != TransferPhase.STOP) {
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
                    !_fileList.isEmpty())
                {
                    throw new IllegalStateException(
                        "received file list DONE when not recursive and file " +
                        "list is not empty: " + _fileList);
                }
                numSegmentsInProgress--;
                if (numSegmentsInProgress <= 0 && _fileList.isEmpty()) {
                    if (!isEOF) {
                        throw new IllegalStateException(
                            "got file list DONE with empty file list and at " +
                            "least all ouststanding segment deletions " +
                            "acknowledged but haven't received file list EOF");
                    }
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine(String.format("phase transition %s -> %s",
                                                phase,
                                                phase.next()));
                    }
                    phase = phase.next();
                    if (phase == TransferPhase.TEAR_DOWN_1) {
                        _generator.processDeferredJobs();
                    }
                    _generator.sendSegmentDone(); // 3 after empty
                } else if (numSegmentsInProgress < 0 && !_fileList.isEmpty()) {
                    throw new IllegalStateException(
                        "Received more acked deleted segments then we have " +
                        "sent to peer: " + _fileList);
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
                if (_fileList.isExpandable()) {
                    throw new IllegalStateException("Received file list EOF " +
                                                    "from peer while having " +
                                                    "an expandable file " +
                                                    "list: " + _fileList);
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
                    _fileList.getStubDirectoryOrNull(directoryIndex);
                if (directory == null) {
                    throw new IllegalStateException(String.format(
                        "there is no stub directory for index %d",
                        directoryIndex));
                }
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                        "Receiving directory index %d is dir %s",
                        directoryIndex, directory));
                }

                segment = receiveSegment(directory);
                if (_isListOnly) {
                    _generator.listSegment(segment);
                } else {
                    _generator.generateSegment(segment);
                }
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

                if (phase != TransferPhase.TRANSFER) {
                    throw new RsyncProtocolException(
                        String.format("Error: wrong phase (%s)",
                                      phase));
                }

                segment = Util.defaultIfNull(segment, _fileList.firstSegment());
                LocatableFileInfo fileInfo = (LocatableFileInfo) segment.getFileWithIndexOrNull(index);
                if (fileInfo == null) {
                    if (_fileSelection != FileSelection.RECURSE) {
                        throw new RsyncProtocolException(String.format(
                            "Received invalid file index %d from peer",
                            index));
                    }
                    segment = _fileList.getSegmentWith(index);
                    if (segment == null) {
                        throw new RsyncProtocolException(String.format(
                            "Received invalid file %d from peer",
                            index));
                    }
                    fileInfo = (LocatableFileInfo) segment.getFileWithIndexOrNull(index);
                    assert fileInfo != null;
                }

                if (_log.isLoggable(Level.INFO)) {
                    _log.info(fileInfo.toString());
                }

                _stats._numTransferredFiles++;
                _stats._totalTransferredSize += fileInfo.attrs().size();

                if (isTransferred(index) && _log.isLoggable(Level.FINE)) {
                    _log.fine("Re-receiving " + fileInfo);
                }
                ioError |= receiveAndMatch(segment, index, fileInfo);
            }
        }
        return ioError;
    }

    private boolean isRemoteAndLocalFileIdentical(Path localFile,
                                                  MessageDigest md,
                                                  LocatableFileInfo fileInfo)
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
                                        fileInfo,
                                        MD5.md5DigestToString(md5sum)));
            } else {
                _log.fine(String.format("%s checksum/size mismatch : " +
                                        "our=%s (size=%d), peer=%s (size=%d)",
                                        fileInfo,
                                        MD5.md5DigestToString(md5sum),
                                        tempSize,
                                        MD5.md5DigestToString(peerMd5sum),
                                        fileInfo.attrs().size()));
            }
        }
        return isIdentical;
    }

    private int moveTempfileToTarget(Path tempFile, Path target)
            throws InterruptedException
    {
        boolean isOK = FileOps.atomicMove(tempFile, target);
        if (isOK) {
            return 0;
        } else {
            String msg = String.format("Error: when moving temporary file %s " +
                                       "to %s", tempFile, target);
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe(msg);
            }
            _generator.sendMessage(MessageCode.ERROR_XFER, msg);
            return IoError.GENERAL;
        }
    }

    private void updateAttrsIfDiffer(Path path, RsyncFileAttributes targetAttrs)
        throws IOException
    {
        RsyncFileAttributes curAttrs = _fileAttributeManager.stat(path);

        if (_isPreservePermissions && curAttrs.mode() != targetAttrs.mode()) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                    "updating file permissions %o -> %o on %s",
                    curAttrs.mode(), targetAttrs.mode(), path));
            }
            _fileAttributeManager.setFileMode(path, targetAttrs.mode(), LinkOption.NOFOLLOW_LINKS);
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
            _fileAttributeManager.setLastModifiedTime(path, targetAttrs.lastModifiedTime(),
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
                _fileAttributeManager.setOwner(path, targetAttrs.user(),
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
                _fileAttributeManager.setUserId(path, targetAttrs.user().id(),
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
                _fileAttributeManager.setGroup(path, targetAttrs.group(),
                                 LinkOption.NOFOLLOW_LINKS);
            } else if ((_isNumericIds ||
                        targetAttrs.group().name().isEmpty()) &&
                       curAttrs.group().id() != targetAttrs.group().id()) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("updating gid %d -> %d on %s",
                                            curAttrs.group().id(),
                                            targetAttrs.group().id(), path));
                }
                _fileAttributeManager.setGroupId(path, targetAttrs.group().id(),
                                   LinkOption.NOFOLLOW_LINKS);
            }
        }
    }

    private int receiveAndMatch(Filelist.Segment segment,
                                int index,
                                LocatableFileInfo fileInfo)
        throws ChannelException, InterruptedException, RsyncProtocolException
    {
        int ioError = 0;
        Checksum.Header checksumHeader = receiveChecksumHeader();
        if (_log.isLoggable(Level.FINE)) {
            _log.fine("received peer checksum " + checksumHeader);
        }

        try (AutoDeletable tempFile = new AutoDeletable(
                Files.createTempFile(fileInfo.path().getParent(),
                                     null, null)))
        {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("created tempfile " + tempFile);
            }
            ioError |= matchData(segment, index, fileInfo, checksumHeader,
                                 tempFile.path());
        } catch (IOException e) {
            String msg = String.format("failed to create tempfile in %s: %s",
                                       fileInfo.path().getParent(),
                                       e.getMessage());
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe(msg);
            }
            _generator.sendMessage(MessageCode.ERROR_XFER, msg + '\n');
            discardData(checksumHeader);
            _in.skip(Checksum.MAX_DIGEST_LENGTH);
            ioError |= IoError.GENERAL;
            _generator.purgeFile(segment, index);
        }
        return ioError;
    }

    private int matchData(Filelist.Segment segment,
                          int index,
                          LocatableFileInfo fileInfo,
                          Checksum.Header checksumHeader,
                          Path tempFile)
        throws ChannelException, InterruptedException, RsyncProtocolException
    {
        int ioError = 0;
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
                    !resultFile.equals(fileInfo.path()))
                {
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine(String.format("moving %s -> %s",
                                                resultFile,
                                                fileInfo.path()));
                    }
                    ioError |= moveTempfileToTarget(resultFile,
                                                    fileInfo.path());
                }
            } catch (IOException e) {
                ioError |= IoError.GENERAL;
                if (_log.isLoggable(Level.SEVERE)) {
                    _log.severe(String.format("failed to update attrs on %s: " +
                                              "%s",
                                              resultFile, e.getMessage()));
                }
            }
            _generator.purgeFile(segment, index);
        } else {
            if (isTransferred(index)) {
                try {
                    ioError |= IoError.GENERAL;
                    _generator.purgeFile(segment, index);
                    _generator.sendMessage(MessageCode.ERROR_XFER,
                                           String.format(
                                                   "%s (index %d) failed " +
                                                   "verification, update " +
                                                   "discarded\n",
                                                   fileInfo.path(),
                                                   index));
                } catch (TextConversionException e) {
                    if (_log.isLoggable(Level.SEVERE)) {
                        _log.log(Level.SEVERE, "", e);
                    }
                }
            } else {
                _generator.generateFile(segment, index, fileInfo);
                setIsTransferred(index);
            }
        }
        return ioError;
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

    private String decodePathName(byte[] pathNameBytes)
            throws InterruptedException, ChannelException
    {
        String pathNameOrNull = _characterDecoder.decodeOrNull(pathNameBytes);
        if (pathNameOrNull == null) {
            try {
                _generator.sendMessage(MessageCode.ERROR,
                        String.format("Error: unable to decode path name of " +
                                      "%s using character set %s. Result " +
                                      "with illegal characters replaced: %s\n",
                                      Text.bytesToString(pathNameBytes),
                                      _characterDecoder.charset(),
                                      new String(pathNameBytes,
                                                 _characterDecoder.charset())));
            } catch (TextConversionException e) {
                if (_log.isLoggable(Level.SEVERE)) {
                    _log.log(Level.SEVERE, "", e);
                }
            }
            _ioError |= IoError.GENERAL;
            return null;
        }
        if (!_isListOnly) {
            String separator = _targetPath.getFileSystem().getSeparator();
            if (!PathOps.isDirectoryStructurePreservable(separator, pathNameOrNull)) {
                try {
                    _generator.sendMessage(MessageCode.ERROR,
                            String.format(
                                    "Illegal file name. \"%s\" contains this " +
                                    "OS' path name separator \"%s\" and " +
                                    "cannot be stored and later retrieved " +
                                    "using the same name again\n",
                                    pathNameOrNull, separator));
                } catch (TextConversionException e) {
                    if (_log.isLoggable(Level.SEVERE)) {
                        _log.log(Level.SEVERE, "", e);
                    }
                }
                _ioError |= IoError.GENERAL;
                return null;
            }
        }
        return pathNameOrNull;
    }

    private int[] receiveDeviceInfo(char flags, RsyncFileAttributes attrs)
            throws ChannelException
    {
        int[] res = { -1, -1 };
        if ((_isPreserveDevices &&
             (attrs.isBlockDevice() || attrs.isCharacterDevice())) ||
            (_isPreserveSpecials && (attrs.isFifo() || attrs.isSocket())))
        {
            if ((flags & TransmitFlags.SAME_RDEV_MAJOR) == 0) {
                res[0] = receiveAndDecodeInt();
                _fileInfoCache.setPrevMajor(res[0]);
            } else {
                res[0] = _fileInfoCache.getPrevMajor();
            }
            res[1] = receiveAndDecodeInt();
        }
        return res;
    }

    private Path resolvePathOrNull(String pathNameOrNull)
            throws RsyncSecurityException, InterruptedException
    {
        if (_isListOnly || pathNameOrNull == null) {
            return null;
        }
        try {
            Path relativePath = _pathResolver.relativePathOf(pathNameOrNull);
            Path fullPath = _pathResolver.fullPathOf(relativePath);
            if (_log.isLoggable(Level.FINER)) {
                _log.finer(String.format(
                        "relative path: %s, full path: %s",
                        relativePath, fullPath));
            }
            if (PathOps.isPathPreservable(fullPath)) {
                return fullPath;
            }
            _generator.sendMessage(MessageCode.ERROR,
                                   String.format("Unable to preserve file " +
                                                 "name for: \"%s\"\n",
                                                 pathNameOrNull));
        } catch (InvalidPathException e) {
            _generator.sendMessage(MessageCode.ERROR, e.getMessage());
        }
        _ioError |= IoError.GENERAL;
        return null;
    }


    private char readFlags() throws ChannelException, RsyncProtocolException
    {
        char flags = (char) (_in.getByte() & 0xFF);
        if ((flags & TransmitFlags.EXTENDED_FLAGS) != 0) {
            flags |= (_in.getByte() & 0xFF) << 8;
            if (flags == (TransmitFlags.EXTENDED_FLAGS |
                          TransmitFlags.IO_ERROR_ENDLIST)) {
                if (!_isSafeFileList) {
                    throw new RsyncProtocolException(
                            "invalid flag " + Integer.toBinaryString(flags));
                }
                int ioError = receiveAndDecodeInt();
                _ioError |= ioError;
                if (_log.isLoggable(Level.WARNING)) {
                    _log.warning(String.format("peer process returned an I/O " +
                                               "error (%d)", ioError));
                }
                _generator.disableDelete();
                // flags are not used anymore
                return 0;
            }
        }
        // might be 0
        return flags;
    }

    private FileInfoStub receiveFileInfoStub(char flags)
            throws InterruptedException, ChannelException,
                   RsyncProtocolException, RsyncSecurityException
    {
        byte[] pathNameBytes = receivePathNameBytes(flags);
        RsyncFileAttributes attrs = receiveRsyncFileAttributes(flags);
        String pathNameOrNull = decodePathName(pathNameBytes);
        if (_log.isLoggable(Level.FINE)) {
            _log.fine(String.format("Receiving file information for %s: %s",
                                    pathNameOrNull, attrs));
        }

        int[] deviceRes = receiveDeviceInfo(flags, attrs);
        int major = deviceRes[0];
        int minor = deviceRes[1];

        String symlinkTargetOrNull = null;
        if (_isPreserveLinks && attrs.isSymbolicLink()) {
            symlinkTargetOrNull = receiveSymlinkTarget();
        }

        FileInfoStub stub = new FileInfoStub();
        stub._pathNameOrNull = pathNameOrNull;
        stub._pathNameBytes = pathNameBytes;
        stub._attrs = attrs;
        stub._symlinkTargetOrNull = symlinkTargetOrNull;
        stub._major = major;
        stub._minor = minor;

        if (_log.isLoggable(Level.FINE)) {
            _log.fine("Finished receiving " + stub);
        }

        return stub;
    }

    private static FileInfo createFileInfo(String pathNameOrNull,
                                           byte[] pathNameBytes,
                                           RsyncFileAttributes attrs,
                                           Path pathOrNull,
                                           String symlinkTargetOrNull,
                                           int major, int minor)
    {
        if (_log.isLoggable(Level.FINE)) {
            _log.fine(String.format(
                    "creating fileinfo given: pathName=%s attrs=%s, path=%s, " +
                    "symlinkTarget=%s, major=%d, minor=%d", pathNameOrNull,
                    attrs, pathOrNull, symlinkTargetOrNull, major, minor));
        }
        if (pathNameOrNull == null) { /* untransferrable */
            return new FileInfoImpl(pathNameOrNull, pathNameBytes, attrs);
        } else if ((attrs.isBlockDevice() || attrs.isCharacterDevice() ||
                    attrs.isFifo() || attrs.isSocket()) &&
                   major >= 0 && minor >= 0) {
            if (pathOrNull == null) {
                return new DeviceInfoImpl(pathNameOrNull, pathNameBytes,
                                          attrs, major, minor);
            }
            return new LocatableDeviceInfoImpl(pathNameOrNull, pathNameBytes,
                                               attrs, major, minor, pathOrNull);
        } else if (attrs.isSymbolicLink() && symlinkTargetOrNull != null) {
            if (pathOrNull == null) {
                return new SymlinkInfoImpl(pathNameOrNull, pathNameBytes,
                                           attrs, symlinkTargetOrNull);
            }
            return new LocatableSymlinkInfoImpl(pathNameOrNull, pathNameBytes,
                                                attrs, symlinkTargetOrNull,
                                                pathOrNull);
        }
        // Note: these might be symlinks or device files:
        if (pathOrNull == null) {
            return new FileInfoImpl(pathNameOrNull, pathNameBytes, attrs);
        }
        return new LocatableFileInfoImpl(pathNameOrNull, pathNameBytes,
                                         attrs, pathOrNull);
    }


    private FileInfo receiveFileInfo(char flags) throws InterruptedException,
                                                        ChannelException,
                                                        RsyncSecurityException,
                                                        RsyncProtocolException
    {
        byte[] pathNameBytes = receivePathNameBytes(flags);
        RsyncFileAttributes attrs = receiveRsyncFileAttributes(flags);
        String pathNameOrNull = decodePathName(pathNameBytes);
        Path fullPathOrNull = resolvePathOrNull(pathNameOrNull);

        if (_log.isLoggable(Level.FINE)) {
            _log.fine(String.format("Receiving file information for %s: %s",
                                    pathNameOrNull, attrs));
        }

        int[] deviceRes = receiveDeviceInfo(flags, attrs);
        int major = deviceRes[0];
        int minor = deviceRes[1];

        String symlinkTargetOrNull = null;
        if (_isPreserveLinks && attrs.isSymbolicLink()) {
            symlinkTargetOrNull = receiveSymlinkTarget();
        }

        FileInfo fileInfo = createFileInfo(pathNameOrNull, pathNameBytes, attrs,
                                           fullPathOrNull, symlinkTargetOrNull,
                                           major, minor);

        if (!(fileInfo instanceof LocatableFileInfo)) {
            _generator.disableDelete();
        }

        if (_log.isLoggable(Level.FINE)) {
            _log.fine("Finished receiving " + fileInfo);
        }

        return fileInfo;
    }


    /**
     * @throws RsyncProtocolException if received file is invalid in some way
     * @throws InterruptedException
     * @throws RsyncSecurityException
     */
    private List<FileInfoStub> receiveFileStubs()
            throws ChannelException, RsyncProtocolException,
                   InterruptedException, RsyncSecurityException
    {
        long numBytesRead = _in.numBytesRead() - _in.numBytesPrefetched();
        List<FileInfoStub> stubs = new ArrayList<>();

        while (true) {
            char flags = readFlags();
            if (flags == 0) {
                break;
            }
            if (_log.isLoggable(Level.FINER)) {
                _log.finer("got flags " + Integer.toBinaryString(flags));
            }
            FileInfoStub stub = receiveFileInfoStub(flags);
            stubs.add(stub);
        }

        long segmentSize = _in.numBytesRead() -
                           _in.numBytesPrefetched() - numBytesRead;
        _stats._totalFileListSize += segmentSize;
        return stubs;
    }

    /**
     * @throws RsyncProtocolException if received file is invalid in some way
     * @throws InterruptedException
     * @throws RsyncSecurityException
     */
    private Filelist.Segment receiveSegment(FileInfo dir)
            throws ChannelException, RsyncProtocolException,
                   InterruptedException, RsyncSecurityException
    {
        long numBytesRead = _in.numBytesRead() - _in.numBytesPrefetched();
        Filelist.SegmentBuilder builder = new Filelist.SegmentBuilder(dir);

        while (true) {
            char flags = readFlags();
            if (flags == 0) {
                break;
            }
            if (_log.isLoggable(Level.FINER)) {
                _log.finer("got flags " + Integer.toBinaryString(flags));
            }
            FileInfo fileInfo = receiveFileInfo(flags);
            builder.add(fileInfo);
        }

        long segmentSize = _in.numBytesRead() -
                           _in.numBytesPrefetched() - numBytesRead;
        _stats._totalFileListSize += segmentSize;
        Filelist.Segment segment = _fileList.newSegment(builder);
        return segment;
    }


    /**
     *
     * @throws TextConversionException
     * @throws ChannelException
     */
    private String receiveSymlinkTarget() throws ChannelException
    {
        int length = receiveAndDecodeInt();
        ByteBuffer buf = _in.get(length);
        String name = _characterDecoder.decode(buf);
        return name;
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
        System.arraycopy(prevFileNameBytes /* src */,
                         0 /* srcPos */,
                         fileNameBytes /* dst */,
                         0 /* dstPos */,
                         prefixNumBytes /* length */);
        _in.get(fileNameBytes, prefixNumBytes, suffixNumBytes);
        _fileInfoCache.setPrevFileNameBytes(fileNameBytes);
        return fileNameBytes;
    }

    private RsyncFileAttributes receiveRsyncFileAttributes(char xflags)
        throws ChannelException, RsyncProtocolException
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
                _recursiveUidUserMap.put(user.id(), user);
            } else if (_fileSelection == FileSelection.RECURSE) {
                // && !isReceiveUsername where isReceiveUserName is only true
                // once for every new mapping, old ones have been sent
                // previously
                int uid = receiveUserId();
                // Note: _uidUserMap contains a predefined mapping for root
                user = _recursiveUidUserMap.get(uid);
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
                _recursiveGidGroupMap.put(group.id(), group);
            } else if (_fileSelection == FileSelection.RECURSE) {
                int gid = receiveGroupId();
                group = _recursiveGidGroupMap.get(gid);
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

    private User getPreviousUser() throws RsyncProtocolException
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

    private Group getPreviousGroup() throws RsyncProtocolException
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

    private User receiveIncompleteUser() throws ChannelException,
                                                RsyncProtocolException
    {
        int uid = receiveUserId();
        return new User("", uid);
    }

    private Group receiveIncompleteGroup() throws ChannelException,
                                                  RsyncProtocolException
    {
        int gid = receiveGroupId();
        return new Group("", gid);
    }

    private int receiveUserId() throws ChannelException, RsyncProtocolException
    {
        int uid = receiveAndDecodeInt();
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("received user id " + uid);
        }
        if (uid < 0 || uid > User.ID_MAX) {
            throw new RsyncProtocolException(String.format(
                "received illegal value for user id: %d (valid range [0..%d]",
                uid, User.ID_MAX));
        }
        return uid;
    }

    private int receiveGroupId() throws ChannelException,
                                        RsyncProtocolException
    {
        int gid = receiveAndDecodeInt();
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("received group id " + gid);
        }
        if (gid < 0 || gid > Group.ID_MAX) {
            throw new RsyncProtocolException(String.format(
                "received illegal value for group id: %d (valid range [0..%d]",
                gid, Group.ID_MAX));
        }
        return gid;
    }

    /**
     * @throws RsyncProtocolException if user name cannot be decoded or it is
     *     the empty string
     */
    private String receiveUserName() throws ChannelException,
                                            RsyncProtocolException
    {
        try {
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
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        }
    }

    /**
     * @throws RsyncProtocolException if group name cannot be decoded or it is
     *     the empty string
     */
    private String receiveGroupName() throws ChannelException,
                                             RsyncProtocolException
    {
        try {
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
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        }
    }

    private User receiveUser() throws ChannelException, RsyncProtocolException
    {
        int uid = receiveUserId();
        String userName = receiveUserName();
        return new User(userName, uid);
    }

    private Group receiveGroup() throws ChannelException, RsyncProtocolException
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
        _stats._totalLiteralSize += sizeLiteral;
        _stats._totalMatchedSize += sizeMatch;
    }

    private Path mergeDataFromPeerAndReplica(LocatableFileInfo fileInfo,
                                             Path tempFile,
                                             Checksum.Header checksumHeader,
                                             MessageDigest md)
            throws ChannelException,
                   InterruptedException,
                   RsyncProtocolException
    {
        assert fileInfo != null;
        assert tempFile != null;
        assert checksumHeader != null;
        assert md != null;

        try (FileChannel target = FileChannel.open(tempFile,
                                                   StandardOpenOption.WRITE)) {
            Path p = fileInfo.path();
            try (FileChannel replica =
                    FileChannel.open(p, StandardOpenOption.READ)) {
                RsyncFileAttributes attrs = _fileAttributeManager.stat(p);
                if (attrs.isRegularFile()) {
                    boolean isIntact = combineDataToFile(replica, target,
                                                         checksumHeader, md);
                    if (isIntact) {
                        RsyncFileAttributes attrs2 = _fileAttributeManager.statOrNull(p);
                        if (FileOps.isDataModified(attrs, attrs2)) {
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
        throws IOException, ChannelException, RsyncProtocolException
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
        _stats._totalLiteralSize += sizeLiteral;
        _stats._totalMatchedSize += sizeMatch;
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

    // NOTE: code duplication with Sender
    public void readAllMessagesUntilEOF() throws ChannelException,
                                                 RsyncProtocolException
    {
        try {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("reading final messages until EOF");
            }
            // dummy read to get any final messages from peer
            byte dummy = _in.getByte();
            // we're not expected to get this far, getByte should throw
            // ChannelEOFException
            ByteBuffer buf = ByteBuffer.allocate(1024);
            try {
                buf.put(dummy);
                _in.get(buf);
            } catch (ChannelEOFException ignored) {
                // ignored
            }
            buf.flip();
            throw new RsyncProtocolException(String.format(
                    "Unexpectedly got %d bytes from peer during connection " +
                    "tear down: %s",
                    buf.remaining(), Text.byteBufferToString(buf)));

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
