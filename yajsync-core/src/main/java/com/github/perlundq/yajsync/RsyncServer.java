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
import java.nio.charset.UnsupportedCharsetException;
import java.util.concurrent.ExecutorService;

import com.github.perlundq.yajsync.internal.session.FilterMode;
import com.github.perlundq.yajsync.internal.session.Generator;
import com.github.perlundq.yajsync.internal.session.Receiver;
import com.github.perlundq.yajsync.internal.session.RsyncTaskExecutor;
import com.github.perlundq.yajsync.internal.session.Sender;
import com.github.perlundq.yajsync.internal.session.ServerSessionConfig;
import com.github.perlundq.yajsync.internal.session.SessionStatus;
import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.util.Util;
import com.github.perlundq.yajsync.server.module.Modules;


public class RsyncServer
{
    public static final int DEFAULT_LISTEN_PORT = 873;

    public static class Builder
    {
        private boolean _isDeferWrite;
        private Charset _charset = Charset.forName(Text.UTF8_NAME);
        private ExecutorService _executorService;

        public Builder isDeferWrite(boolean isDeferWrite)
        {
            _isDeferWrite = isDeferWrite;
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

    private final boolean _isDeferWrite;
    private final Charset _charset;
    private final RsyncTaskExecutor _rsyncTaskExecutor;

    private RsyncServer(Builder builder)
    {
        _isDeferWrite = builder._isDeferWrite;
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
                                                     cfg.checksumSeed(), cfg.checksumHash() ).
                    filterMode(FilterMode.RECEIVE).
                    charset(cfg.charset()).
                    fileSelection(cfg.fileSelection()).
                    isPreserveDevices(cfg.isPreserveDevices()).
                    isPreserveSpecials(cfg.isPreserveSpecials()).
                    isPreserveLinks(cfg.isPreserveLinks()).
                    isPreserveUser(cfg.isPreserveUser()).
                    isPreserveGroup(cfg.isPreserveGroup()).
                    isNumericIds(cfg.isNumericIds()).
                    isInterruptible(isChannelsInterruptible).
                    isSafeFileList(cfg.isSafeFileList()).build();
            return _rsyncTaskExecutor.exec(sender);
        } else {
            Generator generator = new Generator.Builder(out,
                                                        cfg.checksumSeed(), cfg.checksumHash()).
                    charset(cfg.charset()).
                    fileSelection(cfg.fileSelection()).
                    isDelete(cfg.isDelete()).
                    isPreserveDevices(cfg.isPreserveDevices()).
                    isPreserveSpecials(cfg.isPreserveSpecials()).
                    isPreserveLinks(cfg.isPreserveLinks()).
                    isPreservePermissions(cfg.isPreservePermissions()).
                    isPreserveTimes(cfg.isPreserveTimes()).
                    isPreserveUser(cfg.isPreserveUser()).
                    isPreserveGroup(cfg.isPreserveGroup()).
                    isNumericIds(cfg.isNumericIds()).
                    isIgnoreTimes(cfg.isIgnoreTimes()).
                    isAlwaysItemize(cfg.verbosity() > 1).
                    isInterruptible(isChannelsInterruptible).build();
            Receiver receiver = Receiver.Builder.newServer(generator,
                                                           in,
                                                           cfg.getReceiverDestination()).
                    filterMode(cfg.isDelete() ? FilterMode.RECEIVE
                                              : FilterMode.NONE).
                    isDeferWrite(_isDeferWrite).
                    isSafeFileList(cfg.isSafeFileList()).build();
            return _rsyncTaskExecutor.exec(generator, receiver);
        }
    }
}
