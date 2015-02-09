/*
 * Rsync server -> client session creation
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
import java.util.concurrent.ExecutorService;

import com.github.perlundq.yajsync.text.Text;

public class RsyncServerSession
{
    private Charset _charset = Charset.forName(Text.UTF8_NAME);
    private boolean _isDeferredWrite;

    public RsyncServerSession() {}

    public void setCharset(Charset charset)
    {
        _charset = charset;
    }

    public void setIsDeferredWrite(boolean isDeferredWrite)
    {
        _isDeferredWrite = isDeferredWrite;
    }

    public boolean transfer(ExecutorService executor,
                            ReadableByteChannel in,
                            WritableByteChannel out,
                            Modules modules,
                            boolean isChannelsInterruptible)
        throws RsyncException, InterruptedException
    {
        ServerSessionConfig cfg = ServerSessionConfig.handshake(_charset,       // throws IllegalArgumentException if _charset is not supported
                                                                in,
                                                                out,
                                                                modules);

        if (cfg.status() == SessionStatus.ERROR) {
            return false;
        } else if (cfg.status() == SessionStatus.EXIT) {
            return true;
        }

        if (cfg.isSender()) {
            Sender sender = Sender.newServerInstance(in,
                                                     out,
                                                     cfg.sourceFiles(),
                                                     cfg.charset(),
                                                     cfg.checksumSeed()).
                setIsRecursive(cfg.isRecursive()).
                setIsPreserveUser(cfg.isPreserveUser()).
                setIsInterruptible(isChannelsInterruptible).
                setIsSafeFileList(cfg.isSafeFileList());
            return RsyncTaskExecutor.exec(executor, sender);
        } else {
            Generator generator =
                Generator.newServerInstance(out, cfg.charset(),
                                            cfg.checksumSeed()).
                    setIsRecursive(cfg.isRecursive()).
                    setIsPreservePermissions(cfg.isPreservePermissions()).
                    setIsPreserveTimes(cfg.isPreserveTimes()).
                    setIsPreserveUser(cfg.isPreserveUser()).
                    setIsAlwaysItemize(cfg.verbosity() > 1).
                    setIsInterruptible(isChannelsInterruptible);
            Receiver receiver =
                Receiver.newServerInstance(generator, in, cfg.charset(),
                                           cfg.getReceiverDestination().toString()).
                    setIsRecursive(cfg.isRecursive()).
                    setIsPreservePermissions(cfg.isPreservePermissions()).
                    setIsPreserveTimes(cfg.isPreserveTimes()).
                    setIsPreserveUser(cfg.isPreserveUser()).
                    setIsDeferredWrite(_isDeferredWrite).
                    setIsInterruptible(isChannelsInterruptible).
                    setIsSafeFileList(cfg.isSafeFileList());

            return RsyncTaskExecutor.exec(executor, generator,
                                                    receiver);
        }
    }
}
