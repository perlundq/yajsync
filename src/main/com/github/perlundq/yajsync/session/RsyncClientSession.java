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
import java.util.concurrent.ExecutorService;

import com.github.perlundq.yajsync.session.ClientSessionConfig.AuthProvider;
import com.github.perlundq.yajsync.text.Text;

public class RsyncClientSession
{
    private boolean _isDeferredWrite;
    private boolean _isModuleListing;
    private boolean _isPreserveTimes;
    private boolean _isRecursiveTransfer;
    private boolean _isSender;
    private Charset _charset = Charset.forName(Text.UTF8_NAME);
    private int _verbosity;
    private Statistics _statistics = new Statistics();
    private boolean _isPreservePermissions;

    public RsyncClientSession() {}

    public RsyncClientSession setIsPreservePermissions(boolean isPreservedPermissions)
    {
        _isPreservePermissions = isPreservedPermissions;
        return this;
    }

    public RsyncClientSession setIsPreserveTimes(boolean isPreservedTimes)
    {
        _isPreserveTimes = isPreservedTimes;
        return this;
    }

    public RsyncClientSession setCharset(Charset charset)
    {
        _charset = charset;
        return this;
    }

    public RsyncClientSession setIsDeferredWrite(boolean isDeferredWrite)
    {
        _isDeferredWrite = isDeferredWrite;
        return this;
    }

    public RsyncClientSession setIsRecursiveTransfer(boolean isRecursiveTransfer)
    {
        _isRecursiveTransfer = isRecursiveTransfer;
        return this;
    }

    public RsyncClientSession setIsModuleListing(boolean isModuleListing)
    {
        _isModuleListing = isModuleListing;
        return this;
    }

    public RsyncClientSession setIsSender(boolean isSender)
    {
        _isSender = isSender;
        return this;
    }

    public Statistics statistics()
    {
        return _statistics;
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
        if (_isPreservePermissions) {
            sb.append("p");
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

    public boolean transfer(ExecutorService executor,
                            ReadableByteChannel in,
                            WritableByteChannel out,
                            List<String> srcArgs,
                            String dstArg,
                            AuthProvider authProvider,
                            String moduleName,
                            boolean isChannelsInterruptible)
        throws RsyncException, InterruptedException
    {
        List<String> serverArgs = createServerArgs(srcArgs, dstArg);
        ClientSessionConfig cfg =                                               // throws IllegalArgumentException if _charset is not supported
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

        if (_isSender) {
            List<Path> srcPaths = toListOfPaths(srcArgs);
            Sender sender = Sender.newClientInstance(in,
                                                     out,
                                                     srcPaths,
                                                     _charset,
                                                     cfg.checksumSeed()).
                setIsRecursive(_isRecursiveTransfer).
                setIsInterruptible(isChannelsInterruptible);
            boolean isOK = RsyncTaskExecutor.exec(executor, sender);
            _statistics = sender.statistics();
            return isOK;
        } else {
            Generator generator =
                Generator.newClientInstance(out, cfg.charset(),
                                            cfg.checksumSeed()).
                    setIsRecursive(_isRecursiveTransfer).
                    setIsPreservePermissions(_isPreservePermissions).
                    setIsPreserveTimes(_isPreserveTimes).
                    setIsAlwaysItemize(_verbosity > 1).
                    setIsListOnly(_isModuleListing).
                    setIsInterruptible(isChannelsInterruptible);
            Receiver receiver = new Receiver(generator, in, _charset, dstArg).
                setIsSendFilterRules(true).
                setIsReceiveStatistics(true).
                setIsExitEarlyIfEmptyList(true).
                setIsRecursive(_isRecursiveTransfer).
                setIsPreservePermissions(_isPreservePermissions).
                setIsPreserveTimes(_isPreserveTimes).
                setIsListOnly(_isModuleListing).
                setIsDeferredWrite(_isDeferredWrite).
                setIsInterruptible(isChannelsInterruptible).
                setIsExitAfterEOF(true);
            boolean isOK = RsyncTaskExecutor.exec(executor, generator,
                                                         receiver);
            _statistics = receiver.statistics();
            return isOK;
        }
    }
}
