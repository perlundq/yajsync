/*
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
package com.github.perlundq.yajsync;

import java.io.Console;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.session.ClientSessionConfig;
import com.github.perlundq.yajsync.session.ClientSessionConfig.AuthProvider;
import com.github.perlundq.yajsync.session.FileSelection;
import com.github.perlundq.yajsync.session.Generator;
import com.github.perlundq.yajsync.session.Receiver;
import com.github.perlundq.yajsync.session.RsyncException;
import com.github.perlundq.yajsync.session.RsyncTaskExecutor;
import com.github.perlundq.yajsync.session.Sender;
import com.github.perlundq.yajsync.session.SessionStatus;
import com.github.perlundq.yajsync.session.Statistics;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.BitOps;
import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.Util;

public final class RsyncClient
{
    private enum Mode
    {
        LOCAL_COPY, LOCAL_LIST, REMOTE_SEND, REMOTE_RECEIVE, REMOTE_LIST;

        public boolean isList()
        {
            return this == REMOTE_LIST || this == LOCAL_LIST;
        }
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
            return new Result(false, new Statistics());
        }

        public static Result success()
        {
            return new Result(true, new Statistics());
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

    private static Pipe[] pipePair()
    {
        try {
            return new Pipe[] { Pipe.open(), Pipe.open() };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Path> toListOfPaths(Iterable<String> pathNames)
            throws RsyncException
    {
        try {
            List<Path> result = new LinkedList<>();
            for (String pathName : pathNames) {
                result.add(Paths.get(pathName));
            }
            return result;
        } catch (InvalidPathException e) {
            throw new RsyncException(e);
        }
    }

    public class Local
    {
        public class Copy
        {
            private final Iterable<String> _srcPathNames;

            private Copy(Iterable<String> pathNames)
            {
                assert pathNames != null;
                _srcPathNames = pathNames;
            }

            public Result to(String dstPathName) throws RsyncException,
                                                        InterruptedException
            {
                assert dstPathName != null;
                return localTransfer(Mode.LOCAL_COPY, _srcPathNames,
                                     dstPathName);
            }
        }

        public Copy copy(Iterable<String> pathNames)
        {
            assert pathNames != null;
            return new Copy(pathNames);
        }

        public Copy copy(String[] pathNames)
        {
            return copy(Arrays.asList(pathNames));
        }

        public Result list(Iterable<String> pathNames)
                throws RsyncException, InterruptedException
        {
            assert pathNames != null;
            return localTransfer(Mode.LOCAL_LIST, pathNames,
                                 CURRENT_DIR /* dstPathName */);

        }

        public Result list(String[] pathNames)
                throws RsyncException, InterruptedException
        {
            return list(Arrays.asList(pathNames));
        }

        private Result localTransfer(Mode mode, Iterable<String> srcPathNames,
                                     String dstPathName)
                throws RsyncException, InterruptedException
        {
            assert mode != null;
            assert srcPathNames != null;
            assert dstPathName != null;
            Pipe[] pipePair = pipePair();
            Pipe toSender = pipePair[0];
            Pipe toReceiver = pipePair[1];
            List<Path> srcPaths = toListOfPaths(srcPathNames);
            FileSelection fileSelection =
                    Util.defaultIfNull(_fileSelectionOrNull,
                                       mode.isList()
                                           ? FileSelection.TRANSFER_DIRS
                                           : FileSelection.EXACT);
            byte[] seed = BitOps.toLittleEndianBuf((int) System.currentTimeMillis());
            Sender sender = new Sender.Builder(toSender.source(),
                                               toReceiver.sink(),
                                               srcPaths,
                                               seed).
                    isExitEarlyIfEmptyList(true).
                    charset(_charset).
                    isPreserveUser(_isPreserveUser).
                    fileSelection(fileSelection).build();
            Generator generator = new Generator.Builder(toSender.sink(), seed).
                    charset(_charset).
                    fileSelection(fileSelection).
                    stdout(_stdout).
                    isPreservePermissions(_isPreservePermissions).
                    isPreserveTimes(_isPreserveTimes).
                    isPreserveUser(_isPreserveUser).
                    isIgnoreTimes(_isIgnoreTimes).
                    isListOnly(mode.isList()).
                    isAlwaysItemize(_isAlwaysItemize).build();
            Receiver receiver = new Receiver.Builder(generator,
                                                     toReceiver.source(),
                                                     dstPathName).
                    isExitEarlyIfEmptyList(true).
                    isDeferredWrite(_isDeferWrite).build();
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

        public Result list(String moduleName, Iterable<String> pathNames)
                throws RsyncException, InterruptedException
        {
            assert moduleName != null;
            assert pathNames != null;
            return transfer(Mode.REMOTE_LIST,
                            pathNames,
                            CURRENT_DIR, /* dstPathName */
                            moduleName );
        }

        public Result list(String moduleName, String[] pathNames)
                throws RsyncException, InterruptedException
        {
            return list(moduleName, Arrays.asList(pathNames));
        }

        public Result listModules() throws RsyncException, InterruptedException
        {
            Iterable<String> files = Collections.emptyList();
            return transfer(Mode.REMOTE_LIST,
                            files,
                            CURRENT_DIR, /* dstPathName */
                            "" /* moduleName */);
        }

        public Send send(Iterable<String> pathNames)
        {
            assert pathNames != null;
            return new Send(pathNames);
        }

        public Send send(String[] pathNames)
        {
            return send(Arrays.asList(pathNames));
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
            private final Iterable<String> _srcPathNames;

            private Send(Iterable<String> pathNames)
            {
                assert pathNames != null;
                _srcPathNames = pathNames;
            }

            public Result to(String moduleName, String dstPathName)
                    throws RsyncException, InterruptedException
            {
                assert moduleName != null;
                assert dstPathName != null;
                return transfer(Mode.REMOTE_SEND, _srcPathNames, dstPathName,
                                moduleName);
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

            public Result to(String dstPathName)
                    throws RsyncException, InterruptedException
            {
                assert dstPathName != null;
                return transfer(Mode.REMOTE_RECEIVE ,_srcPathNames, dstPathName,
                                _moduleName);
            }
        }

        private List<String> createServerArgs(Mode mode,
                                              FileSelection fileSelection,
                                              Iterable<String> srcArgs,
                                              String dstArg)
        {
            assert mode != null;
            assert fileSelection != null;
            assert srcArgs != null;
            assert dstArg != null;
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
            // revisit (add L) if we add support for symlinks and can set timestamps for symlinks itself
            sb.append("s");
            sb.append("f");
            // revisit if we add support for --iconv
            serverArgs.add(sb.toString());

            serverArgs.add("."); // arg delimiter

            if (mode == Mode.REMOTE_SEND) {
                serverArgs.add(dstArg);
            } else {
                for (String src : srcArgs) {
                    serverArgs.add(src);
                }
            }
            return serverArgs;
        }

        private Result transfer(Mode mode, Iterable<String> srcPathNames,
                                String dstPathName, String moduleName)
                throws RsyncException, InterruptedException
        {
            assert mode != null;
            assert srcPathNames != null;
            assert moduleName != null;
            assert dstPathName != null;
            FileSelection fileSelection =
                    Util.defaultIfNull(_fileSelectionOrNull,
                                       mode.isList()
                                           ? FileSelection.TRANSFER_DIRS
                                           : FileSelection.EXACT);
            List<String> serverArgs = createServerArgs(mode,
                                                       fileSelection,
                                                       srcPathNames,
                                                       dstPathName);

            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("file selection: %s, src: %s, dst: " +
                                        "%s, remote args: %s",
                                        fileSelection, srcPathNames,
                                        dstPathName, serverArgs));
            }

            ClientSessionConfig cfg = new ClientSessionConfig(_in,
                                                              _out,
                                                              _charset,
                                                              fileSelection == FileSelection.RECURSE,
                                                              _stdout,
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
                switch (mode) {
                case REMOTE_RECEIVE:
                case REMOTE_LIST:
                    Generator generator = new Generator.Builder(_out,
                                                                cfg.checksumSeed()).
                            charset(cfg.charset()).
                            stdout(_stdout).
                            fileSelection(fileSelection).
                            isPreservePermissions(_isPreservePermissions).
                            isPreserveTimes(_isPreserveTimes).
                            isPreserveUser(_isPreserveUser).
                            isIgnoreTimes(_isIgnoreTimes).
                            isAlwaysItemize(_verbosity > 1).
                            isListOnly(mode == Mode.REMOTE_LIST).
                            isInterruptible(_isInterruptible).build();
                    Receiver receiver = new Receiver.Builder(generator, _in,
                                                             dstPathName).
                            isDeferredWrite(_isDeferWrite).
                            isExitAfterEOF(true).
                            isExitEarlyIfEmptyList(true).
                            isReceiveStatistics(true).
                            isSafeFileList(cfg.isSafeFileList()).
                            isSendFilterRules(true).build();
                    boolean isOK = _rsyncTaskExecutor.exec(generator, receiver);
                    return new Result(isOK, receiver.statistics());
                case REMOTE_SEND:
                    List<Path> srcPaths = toListOfPaths(srcPathNames);
                    Sender sender = Sender.Builder.newClient(_in,
                                                             _out,
                                                             srcPaths,
                                                             cfg.checksumSeed()).
                            charset(_charset).
                            fileSelection(fileSelection).
                            isPreserveUser(_isPreserveUser).
                            isInterruptible(_isInterruptible).
                            isSafeFileList(cfg.isSafeFileList()).build();
                    isOK = _rsyncTaskExecutor.exec(sender);
                    return new Result(isOK, sender.statistics());
                default:
                    throw new IllegalStateException();
                }
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

    private static class ConsoleAuthProvider
    implements ClientSessionConfig.AuthProvider
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
        private boolean _isIgnoreTimes;
        private boolean _isPreserveUser;
        private boolean _isPreservePermissions;
        private boolean _isPreserveTimes;
        private Charset _charset = Charset.forName(Text.UTF8_NAME);
        private ExecutorService _executorService;
        private FileSelection _fileSelection;
        private int _verbosity;
        private PrintStream _stdout = System.out;
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

        public Builder authProvider(ClientSessionConfig.AuthProvider authProvider)
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

        public Builder isIgnoreTimes(boolean isIgnoreTimes)
        {
            _isIgnoreTimes = isIgnoreTimes;
            return this;
        }

        public Builder isPreserveUser(boolean isPreserveUser)
        {
            _isPreserveUser = isPreserveUser;
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

        public Builder charset(Charset charset)
        {
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

        public Builder stdout(PrintStream stdout)
        {
            _stdout = stdout;
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
    private static final String CURRENT_DIR =
            Environment.getWorkingDirectory().resolve(".").toString();
    private final ClientSessionConfig.AuthProvider _authProvider;
    private final boolean _isAlwaysItemize;
    private final boolean _isDeferWrite;
    private final boolean _isIgnoreTimes;
    private final boolean _isOwnerOfExecutorService;
    private final boolean _isPreserveUser;
    private final boolean _isPreservePermissions;
    private final boolean _isPreserveTimes;
    private final Charset _charset;
    private final ExecutorService _executorService;
    private final FileSelection _fileSelectionOrNull;
    private final int _verbosity;
    private final PrintStream _stdout;
    private final PrintStream _stderr;
    private final RsyncTaskExecutor _rsyncTaskExecutor;

    private RsyncClient(Builder builder)
    {
        assert builder != null;
        _authProvider = builder._authProvider;
        _isAlwaysItemize = builder._isAlwaysItemize;
        _isDeferWrite = builder._isDeferWrite;
        _isIgnoreTimes = builder._isIgnoreTimes;
        _isPreserveUser = builder._isPreserveUser;
        _isPreservePermissions = builder._isPreservePermissions;
        _isPreserveTimes = builder._isPreserveTimes;
        _charset = builder._charset;
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
        _stdout = builder._stdout;
        _stderr = builder._stderr;

    }
}
