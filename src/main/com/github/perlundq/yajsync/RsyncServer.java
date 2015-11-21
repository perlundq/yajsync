/*
 * Rsync server -> client session creation
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
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

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;

import com.github.perlundq.yajsync.session.Generator;
import com.github.perlundq.yajsync.session.Modules;
import com.github.perlundq.yajsync.session.Receiver;
import com.github.perlundq.yajsync.session.RsyncException;
import com.github.perlundq.yajsync.session.RsyncTaskExecutor;
import com.github.perlundq.yajsync.session.Sender;
import com.github.perlundq.yajsync.session.ServerSessionConfig;
import com.github.perlundq.yajsync.session.SessionStatus;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.Util;


public class RsyncServer
{
    public static class Builder
    {
        private boolean _isDeferredWrite;
        private Charset _charset = Charset.forName(Text.UTF8_NAME);
        private ExecutorService _executorService;

        public Builder isDeferredWrite(boolean isDeferredWrite)
        {
            _isDeferredWrite = isDeferredWrite;
            return this;
        }

        /**
         *
         * @throws UnsupportedCharsetException if charset is not supported
         */
        public Builder charset(Charset charset)
        {
            assert charset != null;
            Util.validateCharset(charset);
            _charset = charset;
            return this;
        }

        public RsyncServer build(ExecutorService executorService)
        {
            assert executorService != null;
            _executorService = executorService;
            return new RsyncServer(this);
        }
    }

    private final boolean _isDeferredWrite;
    private final Charset _charset;
    private final RsyncTaskExecutor _rsyncTaskExecutor;

    private RsyncServer(Builder builder)
    {
        _isDeferredWrite = builder._isDeferredWrite;
        _charset = builder._charset;
        _rsyncTaskExecutor = new RsyncTaskExecutor(builder._executorService);
    }

    public boolean serve(Modules modules,
                         ReadableByteChannel in,
                         WritableByteChannel out,
                         boolean isChannelsInterruptible)
        throws RsyncException, InterruptedException
    {
        assert modules != null;
        assert in != null;
        assert out != null;
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
            Sender sender = Sender.Builder.newServer(in, out,
                    cfg.sourceFiles(),
                    cfg.checksumSeed()).
                    charset(cfg.charset()).
                    fileSelection(cfg.fileSelection()).
                    isPreserveUser(cfg.isPreserveUser()).
                    isInterruptible(isChannelsInterruptible).
                    isSafeFileList(cfg.isSafeFileList()).build();
            return _rsyncTaskExecutor.exec(sender);
        } else {
            Generator generator = new Generator.Builder(out,
                    cfg.checksumSeed()).
                    charset(cfg.charset()).
                    fileSelection(cfg.fileSelection()).
                    isPreservePermissions(cfg.isPreservePermissions()).
                    isPreserveTimes(cfg.isPreserveTimes()).
                    isPreserveUser(cfg.isPreserveUser()).
                    isIgnoreTimes(cfg.isIgnoreTimes()).
                    isAlwaysItemize(cfg.verbosity() > 1).
                    isInterruptible(isChannelsInterruptible).build();
            Receiver receiver = Receiver.Builder.newServer(generator,
                    in,
                    cfg.getReceiverDestination().toString()).
                    isDeferredWrite(_isDeferredWrite).
                    isSafeFileList(cfg.isSafeFileList()).build();
            return _rsyncTaskExecutor.exec(generator, receiver);
        }
    }
}
