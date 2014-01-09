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

import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.ui.Module;

public class RsyncServerSession
{
    private static final Logger _log =
        Logger.getLogger(RsyncServerSession.class.getName());
    private Charset _charset = Charset.forName(Text.UTF8_NAME);
    private boolean _isDeferredWrite;
    private Statistics _statistics = new Statistics();

    public RsyncServerSession() {}

    public void setCharset(Charset charset)
    {
        _charset = charset;
    }

    public Charset charset()
    {
        return _charset;
    }

    public void setIsDeferredWrite(boolean isDeferredWrite)
    {
        _isDeferredWrite = isDeferredWrite;
    }

    public boolean isDeferredWrite()
    {
        return _isDeferredWrite;
    }

    private List<Callable<Boolean>> createSenderTasks(final SocketChannel sock, 
                                                      final Charset charset,
                                                      final byte[] checksumSeed,
                                                      final List<Path> srcPaths,
                                                      final boolean isRecursive)
    {
        Callable<Boolean> callableSender = new Callable<Boolean>() {
            @Override
            public Boolean call() throws ChannelException {
                Sender sender = new Sender(sock, sock, srcPaths, charset,
                                           checksumSeed);
                sender.setIsRecursive(isRecursive);
                try {
                    return sender.send(true,  //receiveFilterRules,
                                       true,  //sendStatistics,
                                       true); //exitEarlyIfEmptyList);
                } finally {
                    _statistics = sender.statistics();
                }
            }
        };
        List<Callable<Boolean>> result = new LinkedList<>();
        result.add(callableSender);
        return result;
    }

    private List<Callable<Boolean>> createReceiverTasks(final SocketChannel sock,
                                                        final Charset charset,
                                                        final byte[] checksumSeed,
                                                        final Path destinationPath,
                                                        final boolean isRecursive,
                                                        final boolean isPreserveTimes,
                                                        final boolean isAlwaysItemize,
                                                        final boolean isModuleListing,
                                                        final boolean isDeferredWrite)
    {
        final Generator generator = new Generator(sock, charset, checksumSeed);
        Callable<Boolean> callableGenerator = new Callable<Boolean>() {
            @Override
            public Boolean call() throws ChannelException {
                generator.setIsRecursive(isRecursive);
                generator.setIsPreserveTimes(isPreserveTimes);
                generator.setIsAlwaysItemize(isAlwaysItemize);
                generator.setIsListOnly(isModuleListing);
                return generator.generate();
            }
        };
        Callable<Boolean> callableReceiver = new Callable<Boolean>() {
            @Override
            public Boolean call() throws ChannelException, InterruptedException {
                Receiver receiver = new Receiver(generator, sock,
                                                 destinationPath, charset);
                receiver.setIsRecursive(isRecursive);
                receiver.setIsPreserveTimes(isPreserveTimes);
                receiver.setIsListOnly(isModuleListing);
                receiver.setIsDeferredWrite(isDeferredWrite);
                try {
                    return receiver.receive(false,  // sendFilterRules,
                                             false,  // receiveStatistics,
                                             false); // exitEarlyIfEmptyList);
                } finally {
                    generator.stop();
                    _statistics = receiver.statistics();
                }
            }
        };
        List<Callable<Boolean>> result = new LinkedList<>();
        result.add(callableGenerator);
        result.add(callableReceiver);
        return result;
    }

    // FIXME: BUG _verbosity is not handled correctly
    public boolean startSession(ExecutorService executor,
                                SocketChannel peerChannel,
                                Map<String, Module> modules)
        throws ChannelException
    {
        List<Future<Boolean>> futures = new LinkedList<>();
        try {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("Got connection from " +
                          peerChannel.socket().getRemoteSocketAddress());
            }
            ServerSessionConfig cfg = ServerSessionConfig.handshake(_charset,       // throws IllegalArgumentException if _charset is not supported
                                                                    peerChannel,
                                                                    modules);
            if (cfg.status() != SessionStatus.OK) {
                return false;
            }

            List<Callable<Boolean>> tasks;
            if (cfg.isSender()) {
                tasks = createSenderTasks(peerChannel,
                                          cfg.charset(),
                                          cfg.checksumSeed(),
                                          cfg.sourceFiles(),
                                          cfg.isRecursive()); 
            } else {
                tasks = createReceiverTasks(peerChannel,
                                            cfg.charset(),
                                            cfg.checksumSeed(),
                                            cfg.getReceiverDestination(),
                                            cfg.isRecursive(),
                                            cfg.isPreserveTimes(),
                                            cfg.verbosity() > 1, // isAlwaysItemize,
                                            false,               // isListOnly
                                            _isDeferredWrite);
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
    
    public Statistics statistics()
    {
        return _statistics;
    }
}
