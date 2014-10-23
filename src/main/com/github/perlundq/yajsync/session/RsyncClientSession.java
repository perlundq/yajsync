/*
 * Rsync client -> server session creation
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

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.session.ClientSessionConfig.AuthProvider;
import com.github.perlundq.yajsync.text.Text;

public class RsyncClientSession
{
    private static final Logger _log =
        Logger.getLogger(RsyncClientSession.class.getName());
    private boolean _isDeferredWrite;
    private boolean _isModuleListing;
    private boolean _isPreserveTimes;
    private boolean _isRecursiveTransfer;
    private boolean _isSender;
    private Charset _charset = Charset.forName(Text.UTF8_NAME);
    private int _verbosity;
    private Statistics _statistics = new Statistics();

    public RsyncClientSession() {}

    private List<Callable<Boolean>>
    createSenderTasks(final ReadableByteChannel in,
                      final WritableByteChannel out,
                      final Charset charset,
                      final byte[] checksumSeed,
                      final List<Path> srcPaths)
    {
        Callable<Boolean> callableSender = new Callable<Boolean>() {
            @Override
            public Boolean call() throws ChannelException {
                Sender sender = new Sender(in, out, srcPaths, charset,
                                           checksumSeed);
                sender.setIsRecursive(_isRecursiveTransfer);
                try {
                    boolean isOK = sender.send(false,  // receiveFilterRules,
                                               false,  // sendStatistics,
                                               false); // exitEarlyIfEmptyList);
                    sender.readAllMessagesUntilEOF();
                    return isOK;
                } finally {
                    _statistics = sender.statistics();
                }
            }
        };
        List<Callable<Boolean>> result = new LinkedList<>();
        result.add(callableSender);
        return result;
    }

    private List<Callable<Boolean>>
    createReceiverTasks(final ReadableByteChannel in,
                        final WritableByteChannel out,
                        final Charset charset,
                        final byte[] checksumSeed,
                        final String destinationPathName)

    {
        final Generator generator = new Generator(out, charset, checksumSeed);

        Callable<Boolean> callableGenerator = new Callable<Boolean>() {
            @Override
            public Boolean call() throws ChannelException {
                generator.setIsRecursive(_isRecursiveTransfer);
                generator.setIsPreserveTimes(_isPreserveTimes);
                generator.setIsAlwaysItemize(_verbosity > 1);
                generator.setIsListOnly(_isModuleListing);
                return generator.generate();
            }
        };
        Callable<Boolean> callableReceiver = new Callable<Boolean>() {
            @Override
            public Boolean call() throws InterruptedException, RsyncException
            {
                Receiver receiver = new Receiver(generator, in, charset);
                receiver.setIsRecursive(_isRecursiveTransfer);
                receiver.setIsPreserveTimes(_isPreserveTimes);
                receiver.setIsListOnly(_isModuleListing);
                receiver.setIsDeferredWrite(_isDeferredWrite);
                try {
                    boolean isOK = receiver.receive(destinationPathName,
                                                    true,  // sendFilterRules,
                                                    true,  // receiveStatistics,
                                                    true); // exitEarlyIfEmptyList);
                    receiver.readAllMessagesUntilEOF();
                    return isOK;
                } finally {
                    _statistics = receiver.statistics();
                    generator.stop();
                }
            }
        };
        List<Callable<Boolean>> result = new LinkedList<>();
        result.add(callableGenerator);
        result.add(callableReceiver);
        return result;
    }

    // TODO: rename?
    public boolean startSession(ExecutorService executor,
                                ReadableByteChannel in,
                                WritableByteChannel out,
                                List<String> srcArgs,
                                String dstArg,
                                AuthProvider authProvider,
                                String moduleName)
        throws RsyncException
    {
        List<Future<Boolean>> futures = new LinkedList<>();
        try {
            List<String> serverArgs = createServerArgs(srcArgs, dstArg);
            ClientSessionConfig cfg =                                           // throws IllegalArgumentException if _charset is not supported
                ClientSessionConfig.handshake(in,
                                              out,
                                              _charset,
                                              _isRecursiveTransfer,
                                              moduleName,
                                              serverArgs,
                                              authProvider);

            if (cfg.status() == SessionStatus.ERROR) {
                return false;
            } else if (cfg.status() == SessionStatus.EXIT) {
                return true;
            }

            List<Callable<Boolean>> tasks;
            if (_isSender) {
                List<Path> srcPaths = toListOfPaths(srcArgs);
                tasks = createSenderTasks(in, out, _charset, cfg.checksumSeed(),
                                          srcPaths);
            } else {
                tasks = createReceiverTasks(in, out, _charset,
                                            cfg.checksumSeed(), dstArg);
            }

            CompletionService<Boolean> ecs =
                new ExecutorCompletionService<>(executor);
            for (Callable<Boolean> task : tasks) {
                futures.add(ecs.submit(task));
            }

            boolean isOK = true;
            for (int i = 0; i < tasks.size(); i++) {
                isOK = isOK && ecs.take().get();
            }

            if (_log.isLoggable(Level.FINE)) {
                _log.fine("exit " + (isOK ? "OK" : "ERROR"));
            }
            return isOK;
        } catch (Throwable t) {
            Throwable cause;
            if (t instanceof ExecutionException) {
                cause = t.getCause();
            } else {
                cause = t;
            }
            if (cause instanceof InterruptedException) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("client session thread interrupted");
                }
                return false;
            } else if (cause instanceof ChannelException) {
                throw (ChannelException) cause;
            } else if (cause instanceof RsyncException) {
                throw (RsyncException) cause;
            } else if (cause instanceof Error) {
                throw (Error) cause;
            } else {
                throw (RuntimeException) cause;
            }
        } finally {
            for (Future<Boolean> future : futures) {
                future.cancel(true);
            }
        }
    }

    public void increaseVerbosity()
    {
        _verbosity++;
    }

    public int verbosity()
    {
        return _verbosity;
    }

    public void setIsPreserveTimes(boolean isPreservedTimes)
    {
        _isPreserveTimes = isPreservedTimes;
    }

    public boolean isPreserveTimes()
    {
        return _isPreserveTimes;
    }

    public void setCharset(Charset charset)
    {
        _charset = charset;
    }

    public Charset charset()
    {
        return _charset;
    }

    public Statistics statistics()
    {
        return _statistics;
    }

    public void setIsDeferredWrite(boolean isDeferredWrite)
    {
        _isDeferredWrite = isDeferredWrite;
    }

    public boolean isDeferredWrite()
    {
        return _isDeferredWrite;
    }

    public void setIsRecursiveTransfer(boolean isRecursiveTransfer)
    {
        _isRecursiveTransfer = isRecursiveTransfer;
    }

    public boolean isRecursiveTransfer()
    {
        return _isRecursiveTransfer;
    }

    public void setIsModuleListing(boolean isModuleListing)
    {
        _isModuleListing = isModuleListing;
    }

    public void setIsSender(boolean isSender)
    {
        _isSender = isSender;
    }

    private List<String> createServerArgs(List<String> srcArgs, String dstArg)
    {
        List<String> serverArgs = new LinkedList<>();
        serverArgs.add("--server");
        if (!_isSender) {
            serverArgs.add("--sender");
        }

        StringBuilder sb = new StringBuilder();
        sb.append("-");
        for (int i = 0; i < _verbosity; i++) {
            sb.append("v");
        }
        if (_isModuleListing) {
//            sb.append("d");        // FIXME: BUG: is this really correct
        }
        if (_isPreserveTimes) {
            sb.append("t");
        }
        if (_isRecursiveTransfer) {
            sb.append("r");
        }
        sb.append("e");
        sb.append(".");
        if (_isRecursiveTransfer) {
            sb.append("i");
        }
        // revisit (add L) if we add support for symlinks and can set timestamps for symlinks itself
        sb.append("s");
        sb.append("f");
        // revisit if we add support for --iconv
        serverArgs.add(sb.toString());

        serverArgs.add("."); // arg delimiter

        if (_isSender) {
            serverArgs.add(dstArg);
        } else {
            serverArgs.addAll(srcArgs);
        }

        return serverArgs;
    }

    private static List<Path> toListOfPaths(List<String> pathNames)
    {
        List<Path> result = new LinkedList<>();
        for (String pathName : pathNames) {
            result.add(Paths.get(pathName));
        }
        return result;
    }
}
