/*
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
package com.github.perlundq.yajsync;

import java.io.Console;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.attr.FileInfo;
import com.github.perlundq.yajsync.internal.session.ChecksumHash;
import com.github.perlundq.yajsync.internal.session.ClientSessionConfig;
import com.github.perlundq.yajsync.internal.session.FilterMode;
import com.github.perlundq.yajsync.internal.session.Generator;
import com.github.perlundq.yajsync.internal.session.Receiver;
import com.github.perlundq.yajsync.internal.session.RsyncTaskExecutor;
import com.github.perlundq.yajsync.internal.session.Sender;
import com.github.perlundq.yajsync.internal.session.SessionStatistics;
import com.github.perlundq.yajsync.internal.session.SessionStatus;
import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.util.BitOps;
import com.github.perlundq.yajsync.internal.util.Pair;
import com.github.perlundq.yajsync.internal.util.Util;

public final class RsyncClient
{
    private enum Mode
    {
        LOCAL_COPY, LOCAL_LIST, REMOTE_SEND, REMOTE_RECEIVE, REMOTE_LIST
    }

    public static class Result
    {
        private final boolean _isOK;
        private final Statistics _statistics;

        private Result(boolean isOK, Statistics statistics)
        {
            _isOK = isOK;
            _statistics = statistics;
        }

        public static Result failure()
        {
            return new Result(false, new SessionStatistics());
        }

        public static Result success()
        {
            return new Result(true, new SessionStatistics());
        }

        public boolean isOK()
        {
            return _isOK;
        }

        public Statistics statistics()
        {
            return _statistics;
        }
    }

    public class FileListing
    {
        private final Future<Result> _future;
        private final CountDownLatch _isListingAvailable;
        private BlockingQueue<Pair<Boolean, FileInfo>> _listing;

        private FileListing(final Sender sender,
                            final Generator generator,
                            final Receiver receiver)
        {
            Callable<Result> callable = new Callable<Result>() {
                @Override
                public Result call() throws RsyncException, InterruptedException
                {
                    try {
                        boolean isOK = _rsyncTaskExecutor.exec(sender,
                                                               generator,
                                                               receiver);
                        return new Result(isOK, receiver.statistics());
                    } finally {
                        if (_isOwnerOfExecutorService) {
                            if (_log.isLoggable(Level.FINE)) {
                                _log.fine("shutting down " + _executorService);
                            }
                            _executorService.shutdown();
                        }
                    }
                }
            };
            _future = _executorService.submit(callable);
            _listing = generator.files();
            _isListingAvailable = new CountDownLatch(0);
        }

        private FileListing(final ClientSessionConfig cfg,
                            final String moduleName,
                            final List<String> serverArgs,
                            final AuthProvider authProvider,
                            final ReadableByteChannel in,
                            final WritableByteChannel out,
                            final boolean isInterruptible,
                            final FileSelection fileSelection)

        {
            Callable<Result> callable = new Callable<Result>() {
                @Override
                public Result call() throws RsyncException, InterruptedException
                {
                    try {
                        SessionStatus status = cfg.handshake(moduleName,
                                                             serverArgs,
                                                             authProvider);
                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("handshake status: " + status);
                        }
                        if (status == SessionStatus.ERROR) {
                            return Result.failure();
                        } else if (status == SessionStatus.EXIT) {
                            return Result.success();
                        }
                        Generator generator = new Generator.Builder(out,
                                                                    cfg.checksumSeed(), cfg.checksumHash() ).
                                charset(cfg.charset()).
                                fileSelection(fileSelection).
                                isDelete(_isDelete).
                                isPreserveDevices(_isPreserveDevices).
                                isPreserveSpecials(_isPreserveSpecials).
                                isPreserveLinks(_isPreserveLinks).
                                isPreservePermissions(_isPreservePermissions).
                                isPreserveTimes(_isPreserveTimes).
                                isPreserveUser(_isPreserveUser).
                                isPreserveGroup(_isPreserveGroup).
                                isNumericIds(_isNumericIds).
                                isIgnoreTimes(_isIgnoreTimes).
                                isAlwaysItemize(_verbosity > 1).
                                isInterruptible(isInterruptible).build();
                        _listing = generator.files();
                        _isListingAvailable.countDown();
                        Receiver receiver = Receiver.Builder.newListing(generator, in).
                                filterMode(FilterMode.SEND).
                                isDeferWrite(_isDeferWrite).
                                isExitAfterEOF(true).
                                isExitEarlyIfEmptyList(true).
                                isReceiveStatistics(true).
                                isSafeFileList(cfg.isSafeFileList()).build();
                        boolean isOK = _rsyncTaskExecutor.exec(generator,
                                                               receiver);
                        return new Result(isOK, receiver.statistics());
                    } finally {
                        if (_listing == null) {
                            _listing = new LinkedBlockingQueue<>();
                            _listing.put(new Pair<Boolean, FileInfo>(false, null));
                            _isListingAvailable.countDown();
                        }
                        if (_isOwnerOfExecutorService) {
                            if (_log.isLoggable(Level.FINE)) {
                                _log.fine("shutting down " + _executorService);
                            }
                            _executorService.shutdown();
                        }
                    }
                }
            };
            _future = _executorService.submit(callable);
            _isListingAvailable = new CountDownLatch(1);
        }

        public Future<Result> futureResult()
        {
            return _future;
        }

        public Result get() throws InterruptedException,
                                   RsyncException
        {
            try {
                return _future.get();
            } catch (Throwable e) {
                RsyncTaskExecutor.throwUnwrappedException(e);
                throw new AssertionError();
            }
        }

        /**
         * @return the next available file information or null if there is
         *     nothing left available.
         */
        public FileInfo take() throws InterruptedException
        {
            _isListingAvailable.await();
            Pair<Boolean, FileInfo> res = _listing.take();
            boolean isDone = res.first() == null;
            if (isDone) {
                return null;
            } else {
                return res.second();
            }
        }
    }

    public class ModuleListing
    {
        private final Future<Result> _future;
        private final BlockingQueue<Pair<Boolean, String>> _moduleNames;

        private ModuleListing(final ClientSessionConfig cfg,
                              final List<String> serverArgs)
        {
            Callable<Result> callable = new Callable<Result>() {
                @Override
                public Result call() throws Exception {
                    try {
                        SessionStatus status = cfg.handshake("",
                                                             serverArgs,
                                                             _authProvider);
                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("handshake status: " + status);
                        }
                        if (status == SessionStatus.ERROR) {
                            return Result.failure();
                        } else if (status == SessionStatus.EXIT) {
                            return Result.success();
                        }
                        throw new AssertionError();
                    } finally {
                        if (_isOwnerOfExecutorService) {
                            if (_log.isLoggable(Level.FINE)) {
                                _log.fine("shutting down " + _executorService);
                            }
                            _executorService.shutdown();
                        }
                    }
                }
            };
            _future = _executorService.submit(callable);
            _moduleNames = cfg.modules();
        }

        public Future<Result> futureResult()
        {
            return _future;
        }

        public Result get() throws InterruptedException,
                                   RsyncException
        {
            try {
                return _future.get();
            } catch (Throwable e) {
                RsyncTaskExecutor.throwUnwrappedException(e);
                throw new AssertionError();
            }
        }

        /**
         * @return the next line of the remote module or motd listing or null if
         *     there are no more lines available
         */
        public String take() throws InterruptedException
        {
            Pair<Boolean, String> res = _moduleNames.take();
            boolean isDone = res.first() == null;
            if (isDone) {
                return null;
            } else {
                return res.second();
            }
        }
    }

    private static Pipe[] pipePair()
    {
        try {
            return new Pipe[] { Pipe.open(), Pipe.open() };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public class Local
    {
        public class Copy
        {
            private final Iterable<Path> _srcPaths;

            private Copy(Iterable<Path> srcPaths)
            {
                assert srcPaths != null;
                _srcPaths = srcPaths;
            }

            public Result to(Path dstPath) throws RsyncException,
                                                  InterruptedException
            {
                assert dstPath != null;
                return localTransfer(_srcPaths, dstPath);
            }
        }

        public Copy copy(Iterable<Path> paths)
        {
            assert paths != null;
            return new Copy(paths);
        }

        public Copy copy(Path[] paths)
        {
            return copy(Arrays.asList(paths));
        }

        public FileListing list(Iterable<Path> srcPaths)
        {
            assert srcPaths != null;
            Pipe[] pipePair = pipePair();
            Pipe toSender = pipePair[0];
            Pipe toReceiver = pipePair[1];
            FileSelection fileSelection =
                    Util.defaultIfNull(_fileSelectionOrNull,
                                       FileSelection.TRANSFER_DIRS);
            int seed = (int) System.currentTimeMillis();
            Sender sender = new Sender.Builder(toSender.source(),
                                               toReceiver.sink(),
                                               srcPaths,
                                               seed, _checksumHash).
                    isExitEarlyIfEmptyList(true).
                    charset(_charset).
                    isPreserveDevices(_isPreserveDevices).
                    isPreserveSpecials(_isPreserveSpecials).
                    isPreserveLinks(_isPreserveLinks).
                    isPreserveUser(_isPreserveUser).
                    isPreserveGroup(_isPreserveGroup).
                    isNumericIds(_isNumericIds).
                    fileSelection(fileSelection).build();
            Generator generator = new Generator.Builder(toSender.sink(), seed, _checksumHash).
                    charset(_charset).
                    fileSelection(fileSelection).
                    isDelete(_isDelete).
                    isPreserveDevices(_isPreserveDevices).
                    isPreserveSpecials(_isPreserveSpecials).
                    isPreserveLinks(_isPreserveLinks).
                    isPreservePermissions(_isPreservePermissions).
                    isPreserveTimes(_isPreserveTimes).
                    isPreserveUser(_isPreserveUser).
                    isPreserveGroup(_isPreserveGroup).
                    isNumericIds(_isNumericIds).
                    isIgnoreTimes(_isIgnoreTimes).
                    isAlwaysItemize(_isAlwaysItemize).build();
            Receiver receiver = Receiver.Builder.newListing(generator,
                                                            toReceiver.source()).
                    isExitEarlyIfEmptyList(true).
                    isDeferWrite(_isDeferWrite).build();

            return new FileListing(sender, generator, receiver);
        }

        public FileListing list(Path[] paths)
        {
            return list(Arrays.asList(paths));
        }

        private Result localTransfer(Iterable<Path> srcPaths, Path dstPath)
                throws RsyncException, InterruptedException
        {
            assert srcPaths != null;
            assert dstPath != null;
            Pipe[] pipePair = pipePair();
            Pipe toSender = pipePair[0];
            Pipe toReceiver = pipePair[1];
            FileSelection fileSelection =
                    Util.defaultIfNull(_fileSelectionOrNull,
                                       FileSelection.EXACT);
            int seed = (int) System.currentTimeMillis();
            Sender sender = new Sender.Builder(toSender.source(),
                                               toReceiver.sink(),
                                               srcPaths,
                                               seed, _checksumHash).
                    isExitEarlyIfEmptyList(true).
                    charset(_charset).
                    isPreserveDevices(_isPreserveDevices).
                    isPreserveSpecials(_isPreserveSpecials).
                    isPreserveLinks(_isPreserveLinks).
                    isPreserveUser(_isPreserveUser).
                    isPreserveGroup(_isPreserveGroup).
                    isNumericIds(_isNumericIds).
                    fileSelection(fileSelection).build();
            Generator generator = new Generator.Builder(toSender.sink(), seed, _checksumHash).
                    charset(_charset).
                    fileSelection(fileSelection).
                    isDelete(_isDelete).
                    isPreserveDevices(_isPreserveDevices).
                    isPreserveSpecials(_isPreserveSpecials).
                    isPreserveLinks(_isPreserveLinks).
                    isPreservePermissions(_isPreservePermissions).
                    isPreserveTimes(_isPreserveTimes).
                    isPreserveUser(_isPreserveUser).
                    isPreserveGroup(_isPreserveGroup).
                    isNumericIds(_isNumericIds).
                    isIgnoreTimes(_isIgnoreTimes).
                    isAlwaysItemize(_isAlwaysItemize).build();
            Receiver receiver = new Receiver.Builder(generator,
                                                     toReceiver.source(),
                                                     dstPath).
                    isExitEarlyIfEmptyList(true).
                    isDeferWrite(_isDeferWrite).build();
            try {
                boolean isOK = _rsyncTaskExecutor.exec(sender, generator,
                                                       receiver);
                return new Result(isOK, receiver.statistics());
            } finally {
                if (_isOwnerOfExecutorService) {
                    _executorService.shutdown();
                }
            }
        }
    }

    public class Remote
    {
        private final boolean _isInterruptible;
        private final ReadableByteChannel _in;
        private final WritableByteChannel _out;

        public Remote(ReadableByteChannel in, WritableByteChannel out,
                      boolean isInterruptible)
        {
            assert in != null;
            assert out != null;
            _in = in;
            _out = out;
            _isInterruptible = isInterruptible;
        }

        public FileListing list(String moduleName,
                                Iterable<String> srcPathNames)
        {
            assert moduleName != null;
            assert srcPathNames != null;
            FileSelection fileSelection =
                    Util.defaultIfNull(_fileSelectionOrNull,
                                       FileSelection.TRANSFER_DIRS);
            List<String> serverArgs = createServerArgs(Mode.REMOTE_LIST,
                                                       fileSelection);
            for (String s : srcPathNames) {
                assert s.startsWith(Text.SLASH) : s;
                serverArgs.add(moduleName + s);
            }

            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format(
                        "file selection: %s, src: %s, remote args: %s",
                        fileSelection, srcPathNames, serverArgs));
            }
            ClientSessionConfig cfg = new ClientSessionConfig(_in,
                                                              _out,
                                                              _charset,
                                                              _checksumHash,
                                                              fileSelection == FileSelection.RECURSE,
                                                              _stderr
                                                              );
            return new FileListing(cfg,
                                   moduleName,
                                   serverArgs,
                                   _authProvider,
                                   _in,
                                   _out,
                                   _isInterruptible,
                                   fileSelection);
        }

        public FileListing list(String moduleName, String[] paths)
        {
            return list(moduleName, Arrays.asList(paths));
        }

        public ModuleListing listModules()
        {
            FileSelection fileSelection = FileSelection.EXACT;
            Iterable<String> srcPathNames = Collections.emptyList();

            List<String> serverArgs = createServerArgs(Mode.REMOTE_LIST,
                                                       fileSelection);
            for (String src : srcPathNames) {
                serverArgs.add(src);
            }

            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("file selection: %s, src: %s, remote " +
                                        "args: %s", fileSelection, srcPathNames,
                                        serverArgs));
            }
            ClientSessionConfig cfg = new ClientSessionConfig(_in,
                                                              _out,
                                                              _charset,
                                                              _checksumHash,
                                                              fileSelection == FileSelection.RECURSE,
                                                              _stderr);
            return new ModuleListing(cfg, serverArgs);
        }

        public Send send(Iterable<Path> paths)
        {
            assert paths != null;
            return new Send(paths);
        }

        public Send send(Path[] paths)
        {
            return send(Arrays.asList(paths));
        }

        public Receive receive(String moduleName, Iterable<String> pathNames)
        {
            assert moduleName != null;
            assert pathNames != null;
            return new Receive(moduleName, pathNames);
        }

        public Receive receive(String moduleName, String[] pathNames)
        {
            return receive(moduleName, Arrays.asList(pathNames));
        }

        public class Send
        {
            private final Iterable<Path> _srcPaths;

            private Send(Iterable<Path> srcPaths)
            {
                assert srcPaths != null;
                _srcPaths = srcPaths;
            }

            public Result to(String moduleName, String dstPathName)
                    throws RsyncException, InterruptedException
            {
                assert moduleName != null;
                assert dstPathName != null;
                assert dstPathName.startsWith(Text.SLASH);

                FileSelection fileSelection =
                        Util.defaultIfNull(_fileSelectionOrNull,
                                           FileSelection.EXACT);
                List<String> serverArgs = createServerArgs(Mode.REMOTE_SEND,
                                                           fileSelection);
                serverArgs.add(moduleName + dstPathName);

                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format(
                            "file selection: %s, src: %s, dst: %s, remote " +
                            "args: %s",
                            fileSelection, _srcPaths, dstPathName, serverArgs));
                }

                ClientSessionConfig cfg = new ClientSessionConfig(_in,
                                                                  _out,
                                                                  _charset,
                                                                  _checksumHash,
                                                                  fileSelection == FileSelection.RECURSE,
                                                                  _stderr);
                SessionStatus status = cfg.handshake(moduleName, serverArgs,
                                                     _authProvider);
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("handshake status: " + status);
                }

                if (status == SessionStatus.ERROR) {
                    return Result.failure();
                } else if (status == SessionStatus.EXIT) {
                    return Result.success();
                }

                try {
                    Sender sender = Sender.Builder.newClient(_in,
                                                             _out,
                                                             _srcPaths,
                                                             cfg.checksumSeed(), cfg.checksumHash()).
                            filterMode(_isDelete ? FilterMode.SEND
                                                 : FilterMode.NONE).
                            charset(_charset).
                            fileSelection(fileSelection).
                            isPreserveLinks(_isPreserveLinks).
                            isPreserveUser(_isPreserveUser).
                            isPreserveGroup(_isPreserveGroup).
                            isNumericIds(_isNumericIds).
                            isInterruptible(_isInterruptible).
                            isSafeFileList(cfg.isSafeFileList()).build();
                    boolean isOK = _rsyncTaskExecutor.exec(sender);
                    return new Result(isOK, sender.statistics());
                } finally {
                    if (_isOwnerOfExecutorService) {
                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("shutting down " + _executorService);
                        }
                        _executorService.shutdown();
                    }
                }
            }
        }

        public class Receive
        {
            private final String _moduleName;
            private final Iterable<String> _srcPathNames;

            private Receive(String moduleName, Iterable<String> srcPathNames)
            {
                assert moduleName != null;
                assert srcPathNames != null;
                _moduleName = moduleName;
                _srcPathNames = srcPathNames;
            }

            public Result to(Path dstPath)
                    throws RsyncException, InterruptedException
            {
                assert dstPath != null;
                FileSelection fileSelection =
                        Util.defaultIfNull(_fileSelectionOrNull,
                                           FileSelection.EXACT);

                List<String> serverArgs = createServerArgs(Mode.REMOTE_RECEIVE,
                                                           fileSelection);
                for (String s : _srcPathNames) {
                    assert s.startsWith(Text.SLASH) : s;
                    serverArgs.add(_moduleName + s);
                }

                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("file selection: %s, src: %s, dst: " +
                                            "%s, remote args: %s",
                                            fileSelection, _srcPathNames,
                                            dstPath, serverArgs));
                }

                ClientSessionConfig cfg = new ClientSessionConfig(_in,
                                                                  _out,
                                                                  _charset,
                                                                  _checksumHash,
                                                                  fileSelection == FileSelection.RECURSE,
                                                                  _stderr);
                SessionStatus status = cfg.handshake(_moduleName, serverArgs,
                                                     _authProvider);
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("handshake status: " + status);
                }

                if (status == SessionStatus.ERROR) {
                    return Result.failure();
                } else if (status == SessionStatus.EXIT) {
                    return Result.success();
                }

                try {
                    Generator generator = new Generator.Builder(_out,
                                                                cfg.checksumSeed(), cfg.checksumHash()).
                            charset(cfg.charset()).
                            fileSelection(fileSelection).
                            isDelete(_isDelete).
                            isPreserveLinks(_isPreserveLinks).
                            isPreservePermissions(_isPreservePermissions).
                            isPreserveTimes(_isPreserveTimes).
                            isPreserveUser(_isPreserveUser).
                            isPreserveGroup(_isPreserveGroup).
                            isNumericIds(_isNumericIds).
                            isIgnoreTimes(_isIgnoreTimes).
                            isAlwaysItemize(_verbosity > 1).
                            isInterruptible(_isInterruptible).build();
                    Receiver receiver = new Receiver.Builder(generator, _in,
                                                             dstPath).
                            filterMode(FilterMode.SEND).
                            isDeferWrite(_isDeferWrite).
                            isExitAfterEOF(true).
                            isExitEarlyIfEmptyList(true).
                            isReceiveStatistics(true).
                            isSafeFileList(cfg.isSafeFileList()).build();
                    boolean isOK = _rsyncTaskExecutor.exec(generator, receiver);
                    return new Result(isOK, receiver.statistics());
                } finally {
                    if (_isOwnerOfExecutorService) {
                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("shutting down " + _executorService);
                        }
                        _executorService.shutdown();
                    }
                }
            }
        }

        List<String> toListOfStrings(Iterable<Path> paths)
        {
            List<String> srcPathNames = new LinkedList<>();
            for (Path p : paths) {
                srcPathNames.add(p.toString());
            }
            return srcPathNames;
        }

        private List<String> createServerArgs(Mode mode,
                                              FileSelection fileSelection)
        {
            assert mode != null;
            assert fileSelection != null;
            List<String> serverArgs = new LinkedList<>();
            serverArgs.add("--server");
            boolean isPeerSender = mode != Mode.REMOTE_SEND;
            if (isPeerSender) {
                serverArgs.add("--sender");
            }

            StringBuilder sb = new StringBuilder();
            sb.append("-");
            for (int i = 0; i < _verbosity; i++) {
                sb.append("v");
            }
            if (_isPreserveLinks) {
                sb.append("l");
            }
            if (fileSelection == FileSelection.TRANSFER_DIRS) {
                sb.append("d");
            }
            if (_isPreservePermissions) {
                sb.append("p");
            }
            if (_isPreserveTimes) {
                sb.append("t");
            }
            if (_isPreserveUser) {
                sb.append("o");
            }
            if (_isPreserveGroup) {
                sb.append("g");
            }
            if (_isPreserveDevices) {
                sb.append("D");
            }
            if (_isIgnoreTimes) {
                sb.append("I");
            }
            if (fileSelection == FileSelection.RECURSE) {
                sb.append("r");
            }
            sb.append("e");
            sb.append(".");
            if (fileSelection == FileSelection.RECURSE) {
                sb.append("i");
            }
            sb.append("s");
            sb.append("f");
            serverArgs.add(sb.toString());

            if (_isDelete && mode == Mode.REMOTE_SEND) {
                serverArgs.add("--delete");
            }
            if (_isNumericIds) {
                serverArgs.add("--numeric-ids");
            }
            if (_isDelete &&
                _fileSelectionOrNull == FileSelection.TRANSFER_DIRS)
            {
                // seems like it's only safe to use --delete and --dirs with
                // rsync versions that happens to support --no-r
                serverArgs.add("--no-r");
            }
            if (_isPreserveDevices && !_isPreserveSpecials) {
                serverArgs.add("--no-specials");
            }
            if ( _checksumHash != ChecksumHash.md5 ) {
                serverArgs.add("--checksum-choice=" + _checksumHash );
            }

            serverArgs.add("."); // arg delimiter

            return serverArgs;
        }
    }

    private static class ConsoleAuthProvider implements AuthProvider
    {
        private final Console console = System.console();

        @Override
        public String getUser() throws IOException
        {
            if (console == null) {
                throw new IOException("no console available");
            }
            return console.readLine("User name: ");
        }

        @Override
        public char[] getPassword() throws IOException
        {
            if (console == null) {
                throw new IOException("no console available");
            }
            return console.readPassword("Password: ");
        }
    }

    public static class Builder
    {
        private AuthProvider _authProvider = new ConsoleAuthProvider();
        private boolean _isAlwaysItemize;
        private boolean _isDeferWrite;
        private boolean _isDelete;
        private boolean _isIgnoreTimes;
        private boolean _isPreserveDevices;
        private boolean _isPreserveSpecials;
        private boolean _isPreserveLinks;
        private boolean _isPreserveUser;
        private boolean _isPreserveGroup;
        private boolean _isNumericIds;
        private boolean _isPreservePermissions;
        private boolean _isPreserveTimes;
        private ChecksumHash _checksumHash = ChecksumHash.xxhash;
        private Charset _charset = Charset.forName(Text.UTF8_NAME);
        private ExecutorService _executorService;
        private FileSelection _fileSelection;
        private int _verbosity;
        private PrintStream _stderr = System.err;

        public Local buildLocal()
        {
            return new RsyncClient(this).new Local();
        }

        public Remote buildRemote(ReadableByteChannel in,
                                  WritableByteChannel out,
                                  boolean isInterruptible)
        {
            return new RsyncClient(this).new Remote(in, out, isInterruptible);
        }

        public Builder authProvider(AuthProvider authProvider)
        {
            _authProvider = authProvider;
            return this;
        }

        public Builder isAlwaysItemize(boolean isAlwaysItemize)
        {
            _isAlwaysItemize = isAlwaysItemize;
            return this;
        }

        public Builder isDeferWrite(boolean isDeferWrite)
        {
            _isDeferWrite = isDeferWrite;
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

        public Builder isPreserveDevices(boolean isPreserveDevices)
        {
            _isPreserveDevices = isPreserveDevices;
            return this;
        }

        public Builder isPreserveSpecials(boolean isPreserveSpecials)
        {
            _isPreserveSpecials = isPreserveSpecials;
            return this;
        }

        public Builder isPreserveLinks(boolean isPreserveLinks)
        {
            _isPreserveLinks = isPreserveLinks;
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
        
        public Builder checksumHash( ChecksumHash hash ) {
            _checksumHash = hash;
            return this;
        }

        /**
         *
         * @throws UnsupportedCharsetException if charset is not supported
         */
        public Builder charset(Charset charset)
        {
            Util.validateCharset(charset);
            _charset = charset;
            return this;
        }

        public Builder executorService(ExecutorService executorService)
        {
            _executorService = executorService;
            return this;
        }

        public Builder fileSelection(FileSelection fileSelection)
        {
            _fileSelection = fileSelection;
            return this;
        }

        public Builder stderr(PrintStream stderr)
        {
            _stderr = stderr;
            return this;
        }

        public Builder verbosity(int verbosity)
        {
            _verbosity = verbosity;
            return this;
        }
    }

    private static final Logger _log =
            Logger.getLogger(RsyncClient.class.getName());
    private final AuthProvider _authProvider;
    private final boolean _isAlwaysItemize;
    private final boolean _isDeferWrite;
    private final boolean _isDelete;
    private final boolean _isIgnoreTimes;
    private final boolean _isOwnerOfExecutorService;
    private final boolean _isPreserveDevices;
    private final boolean _isPreserveSpecials;
    private final boolean _isPreserveLinks;
    private final boolean _isPreserveUser;
    private final boolean _isPreserveGroup;
    private final boolean _isNumericIds;
    private final boolean _isPreservePermissions;
    private final boolean _isPreserveTimes;
    private final Charset _charset;
    private final ChecksumHash _checksumHash;
    private final ExecutorService _executorService;
    private final FileSelection _fileSelectionOrNull;
    private final int _verbosity;
    private final PrintStream _stderr;
    private final RsyncTaskExecutor _rsyncTaskExecutor;

    private RsyncClient(Builder builder)
    {
        assert builder != null;
        _authProvider = builder._authProvider;
        _isAlwaysItemize = builder._isAlwaysItemize;
        _isDeferWrite = builder._isDeferWrite;
        _isDelete = builder._isDelete;
        _isIgnoreTimes = builder._isIgnoreTimes;
        _isPreserveDevices = builder._isPreserveDevices;
        _isPreserveSpecials = builder._isPreserveSpecials;
        _isPreserveUser = builder._isPreserveUser;
        _isPreserveGroup = builder._isPreserveGroup;
        _isPreserveLinks = builder._isPreserveLinks;
        _isNumericIds = builder._isNumericIds;
        _isPreservePermissions = builder._isPreservePermissions;
        _isPreserveTimes = builder._isPreserveTimes;
        _charset = builder._charset;
        _checksumHash = builder._checksumHash;
        if (builder._executorService == null) {
            _executorService = Executors.newCachedThreadPool();
            _isOwnerOfExecutorService = true;
        } else {
            _executorService = builder._executorService;
            _isOwnerOfExecutorService = false;
        }
        _rsyncTaskExecutor = new RsyncTaskExecutor(_executorService);
        _fileSelectionOrNull = builder._fileSelection;
        _verbosity = builder._verbosity;
        _stderr = builder._stderr;
    }
}
