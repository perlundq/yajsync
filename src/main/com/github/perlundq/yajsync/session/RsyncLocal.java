/*
 * Rsync local transfer session creation
 *
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

import java.nio.channels.Pipe;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.BitOps;

public class RsyncLocal
{
    private static final Logger _log =
        Logger.getLogger(RsyncLocal.class.getName());
    private int _verbosity;
    private boolean _isRecursiveTransfer;
    private boolean _isPreserveTimes;
    private boolean _isDeferredWrite;
    private Charset _charset = Charset.forName(Text.UTF8_NAME);
    private Statistics _statistics = new Statistics();

    public RsyncLocal() {}
                          
    public void setVerbosity(int verbosity)
    {
        _verbosity = verbosity;
    }

    public void setCharset(Charset charset)
    {
        _charset = charset;
    }

    public void setIsRecursiveTransfer(boolean isRecursiveTransfer)
    {
        _isRecursiveTransfer = isRecursiveTransfer;
    }

    public void setIsPreserveTimes(boolean isPreserveTimes)
    {
        _isPreserveTimes = isPreserveTimes;
    }

    public void setIsDeferredWrite(boolean isDeferredWrite)
    {
        _isDeferredWrite = isDeferredWrite;
    }

    public boolean transfer(ExecutorService executor,
                            Iterable<Path> srcPaths,
                            final String destinationPathName)
        throws ChannelException
    {
        List<Future<Boolean>> futures = new LinkedList<>();
        try {
            byte[] checksumSeed =
                BitOps.toLittleEndianBuf((int) System.currentTimeMillis());

            Pipe toSender = Pipe.open();           // throws IOException
            Pipe toReceiver = Pipe.open();         // throws IOException
            final Sender sender = new Sender(toSender.source(),
                                             toReceiver.sink(),
                                             srcPaths,
                                             _charset,
                                             checksumSeed);
            sender.setIsRecursive(_isRecursiveTransfer);
            final Generator generator = new Generator(toSender.sink(), _charset,
                                                      checksumSeed);
            generator.setIsRecursive(_isRecursiveTransfer);
            generator.setIsPreserveTimes(_isPreserveTimes);
            generator.setIsAlwaysItemize(_verbosity > 1);
            generator.setIsListOnly(false);
            final Receiver receiver = new Receiver(generator,
                                                   toReceiver.source(),
                                                   _charset);
            receiver.setIsRecursive(_isRecursiveTransfer);
            receiver.setIsPreserveTimes(_isPreserveTimes);
            receiver.setIsListOnly(false);
            receiver.setIsDeferredWrite(_isDeferredWrite);

            final boolean transferFilterRules = false;
            final boolean transferStatistics = false;
            final boolean exitEarlyIfEmptyList = true;
            
            ExecutorCompletionService<Boolean> ecs =
                new ExecutorCompletionService<>(executor);
            futures.add(ecs.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws ChannelException {
                    return generator.generate();
                }
            }));
            futures.add(ecs.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws ChannelException,
                                             InterruptedException {
                    try {
                        return receiver.receive(destinationPathName,
                                                transferFilterRules,
                                                transferStatistics,
                                                exitEarlyIfEmptyList);
                    } finally {
                        generator.stop();
                    }
                }
            }));
            // NOTE: also updates _statistics
            futures.add(ecs.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws ChannelException {
                    try {
                        return sender.send(transferFilterRules,
                                           transferStatistics,
                                           exitEarlyIfEmptyList);
                    } finally {
                        _statistics = sender.statistics();
                    }
                }
            }));

            boolean isFirstOK = ecs.take().get();
            boolean isSecondOK = ecs.take().get();
            boolean isThirdOK = ecs.take().get();
            return isFirstOK && isSecondOK && isThirdOK;
            /**
             * ArgumentParsingError      - not propagated
             * ChannelEOFException       - connection closed prematurely
             * ChannelException          - connection failure
             * ChunkOverflow             - not propagated
             * EOFException              - not propagated
             * FileViewNotFound          - not propagated
             * FileViewOpenFailed        - not propagated
             * FileViewReadError         - not propagated
             * IllegalArgumentException  - BUG
             * IllegalStateException     - BUG
             * InvalidPathException      - not propagated
             * OverflowException         - not propagated
             * RsyncProtocolException    - peer does not follow protocol
             * RsyncSecurityException    - peer does not follow protocol, has security implications
             * RuntimeException          - BUG
             * RuntimeInterruptException - not propagated
             * TextConversionException
             */
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
