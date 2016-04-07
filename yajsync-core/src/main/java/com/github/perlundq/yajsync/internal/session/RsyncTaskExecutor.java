/*
 * Copyright (C) 2014 Per Lundqvist
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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.RsyncException;
import com.github.perlundq.yajsync.internal.channels.ChannelException;

public final class RsyncTaskExecutor
{
    private static final Logger _log =
        Logger.getLogger(RsyncTaskExecutor.class.getName());
    private final Executor _executor;

    public RsyncTaskExecutor(Executor executor)
    {
        assert executor != null;
        _executor = executor;
    }

    public boolean exec(RsyncTask... tasks)
        throws RsyncException,InterruptedException
    {
        CompletionService<Boolean> ecs =
            new ExecutorCompletionService<>(_executor);

        List<Future<Boolean>> futures = new LinkedList<>();
        for (RsyncTask task : tasks) {
            futures.add(ecs.submit(task));
        }

        Throwable thrown = null;
        boolean isOK = true;

        for (int i = 0; i < futures.size(); i++) {
            try {
                if (_log.isLoggable(Level.FINER)) {
                    _log.finer(String.format("waiting for result from task " +
                                             "%d/%d", i + 1, futures.size()));
                }
                boolean isThreadOK = ecs.take().get();                          // take throws InterruptedException, get throws CancellationException
                isOK = isOK && isThreadOK;
                if (_log.isLoggable(Level.FINER)) {
                    _log.finer(String.format("task %d/%d finished %s",
                                             i + 1, futures.size(),
                                             isThreadOK ? "OK" : "ERROR"));
                }
            } catch (Throwable t) {
                if (_log.isLoggable(Level.FINER)) {
                    _log.finer(String.format(
                        "deferring exception raised by task %d/%d: %s",
                        i + 1, futures.size(), t));
                }
                if (thrown == null) {
                    thrown = t;
                    for (Future<Boolean> future : futures) {
                        future.cancel(true);
                    }
                    for (RsyncTask task : tasks) {
                        if (!task.isInterruptible()) {
                            try {
                                task.closeChannel();
                            } catch (ChannelException e){
                                t.addSuppressed(e);
                            }
                        }
                    }
                } else {
                    thrown.addSuppressed(t);
                }
            }
        }

        boolean isException = thrown != null;
        if (isException) {
            throwUnwrappedException(thrown);
        }

        if (_log.isLoggable(Level.FINE)) {
            _log.fine("exit " + (isOK ? "OK" : "ERROR"));
        }

        return isOK;
    }

    public static void throwUnwrappedException(Throwable thrown)
        throws InterruptedException, RsyncException
    {
        if (thrown instanceof ExecutionException) {
            thrown = thrown.getCause();                                         // NOTE: thrown is reassigned here
        }

        if (thrown instanceof InterruptedException) {
            throw (InterruptedException) thrown;
        } else if (thrown instanceof ChannelException) {
            throw (ChannelException) thrown;
        } else if (thrown instanceof RsyncException) {
            throw (RsyncException) thrown;
        } else if (thrown instanceof RuntimeException) {                        // e.g. CancellationException
            throw (RuntimeException) thrown;
        } else if (thrown instanceof Error) {
            throw (Error) thrown;
        }
        throw new AssertionError("BUG - missing statement for " + thrown);
    }
}
