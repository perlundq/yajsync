/*
 * A simple rsync command line server implementation
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
package com.github.perlundq.yajsync.ui;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.security.Principal;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.security.AddressPrincipal;
import com.github.perlundq.yajsync.session.ModuleException;
import com.github.perlundq.yajsync.session.ModuleProvider;
import com.github.perlundq.yajsync.session.Modules;
import com.github.perlundq.yajsync.session.RsyncServerSession;
import com.github.perlundq.yajsync.session.Statistics;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.ArgumentParser;
import com.github.perlundq.yajsync.util.ArgumentParsingError;
import com.github.perlundq.yajsync.util.Consts;
import com.github.perlundq.yajsync.util.Option;
import com.github.perlundq.yajsync.util.Util;

public class YajSyncServer
{
    private static final Logger _log =
        Logger.getLogger(YajSyncServer.class.getName());
    private static final int THREAD_FACTOR = 4;

    private boolean _isDeferredWrite;
    private Charset _charset = Charset.forName(Text.UTF8_NAME);
    private int _numThreads = Runtime.getRuntime().availableProcessors() *
                              THREAD_FACTOR;
    private int _port = Consts.DEFAULT_LISTEN_PORT;
    private int _verbosity;
    private InetAddress _address = InetAddress.getLoopbackAddress();

    public YajSyncServer() {}

    private Iterable<Option> options()
    {
        List<Option> options = new LinkedList<>();
        options.add(Option.newStringOption(Option.Policy.OPTIONAL,
                                           "charset", "",
                                           String.format("which charset to " +
                                                         "use (default %s)",
                                                         _charset),
            new Option.Handler() {
                @Override
                public void handle(Option option) throws ArgumentParsingError {
                    String charsetName = (String) option.getValue();
                    try {
                        _charset = Charset.forName(charsetName);
                    } catch (IllegalCharsetNameException |
                             UnsupportedCharsetException e) {
                        throw new ArgumentParsingError(String.format(
                            "failed to set character set to %s: %s",
                            charsetName, e.getMessage()));
                    }
                    if (!Util.isValidCharset(_charset)) {
                        throw new ArgumentParsingError(String.format(
                            "character set %s is not supported - cannot " +
                            "encode SLASH (/), DOT (.), NEWLINE (\n), " +
                            "CARRIAGE RETURN (\r) and NULL (\0) to their " +
                            "ASCII counterparts and vice versa", charsetName));
                    }
                }}));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL,
                                              "verbose", "v",
                                              String.format("output verbosity" +
                                                            " (default %d)",
                                                            _verbosity),
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _verbosity++;
                }}));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL,
                                           "address", "",
                                           String.format("address to bind to" +
                                                         "(default %s)",
                                                         _address),
            new Option.Handler() {
                @Override public void handle(Option option) throws ArgumentParsingError {
                    try {
                        _address = InetAddress.getByName((String) option.getValue());
                    } catch (UnknownHostException e) {
                        throw new ArgumentParsingError(e);
                    }
                }}));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL,
                                            "port", "",
                                            String.format("port number to " +
                                                          "listen on (default" +
                                                          " %d)", _port),
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _port = (int) option.getValue();
                }}));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL,
                                            "threads", "",
                                            String.format("size of thread " +
                                                          "pool (default %d)",
                                                          _numThreads),
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _numThreads = (int) option.getValue();
                }}));

        String deferredWriteHelp = String.format(
            "receiver defers writing into target tempfile as long as " +
            "possible to reduce I/O, at the cost of highly increased risk of the " +
            "file being modified by a process already having it opened " +
            "(default %s)",
            _isDeferredWrite);
        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL,
                                              "defer-write", "",
                                              deferredWriteHelp,
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _isDeferredWrite = true;
                }}));
        return options;
    }

    private Callable<Boolean> createCallable(final ExecutorService executor,
                                             final SocketChannel sock,
                                             final Modules modules)
    {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() {
                boolean isOK;
                try {
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine("connected to " + sock.getRemoteAddress());
                    }

                    RsyncServerSession session = new RsyncServerSession();
                    session.setCharset(_charset);
                    session.setIsDeferredWrite(_isDeferredWrite);
                    isOK = session.startSession(executor,
                                                sock,    // in
                                                sock,    // out
                                                modules);
//                    showStatistics(session.statistics());
                } catch (ChannelException e) {
                    if (_log.isLoggable(Level.SEVERE)) {
                        _log.severe("Error: communication closed with peer: " +
                                    e.getMessage());
                    }
                    isOK = false;
                } catch (Throwable t) {
                    if (_log.isLoggable(Level.SEVERE)) {
                        _log.log(Level.SEVERE, "", t);
                    }
                    isOK = false;
                } finally {
                    try {
                        sock.close();
                    } catch (IOException e) {
                        if (_log.isLoggable(Level.SEVERE)) {
                            _log.severe(String.format(
                                "Got error during close of socket %s: %s",
                                sock, e.getMessage()));
                        }
                        isOK = false;
                    }
                }

                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("Thread exit status: " + (isOK ? "OK" : "ERROR"));
                }
                return isOK;
            }
        };
    }

    private static void showStatistics(Statistics stats)
    {
        System.out.format("Number of files: %d%n" +
            "Number of files transferred: %d%n" +
            "Total file size: %d bytes%n" +
            "Total transferred file size: %d bytes%n" +
            "Literal data: %d bytes%n" +
            "Matched data: %d bytes%n" +
            "File list size: %d%n" +
            "File list generation time: %.3f seconds%n" +
            "File list transfer time: %.3f seconds%n" +
            "Total bytes sent: %d%n" +
            "Total bytes received: %d%n",
            stats.numFiles(),
            stats.numTransferredFiles(),
            stats.totalFileSize(),
            stats.totalTransferredSize(),
            stats.totalLiteralSize(),
            stats.totalMatchedSize(),
            stats.totalFileListSize(),
            stats.fileListBuildTime() / 1000.0,
            stats.fileListTransferTime() / 1000.0,
            stats.totalWritten(),
            stats.totalRead());
    }

    public void start(String[] args) throws IOException, InterruptedException
    {
        ArgumentParser argsParser =
            ArgumentParser.newNoUnnamed(getClass().getSimpleName());
        ModuleProvider moduleProvider = ModuleProvider.getDefault();
        try {
            argsParser.addHelpTextDestination(System.out);
            for (Option o : options()) {
                argsParser.add(o);
            }
            for (Option o : moduleProvider.options()) {
                argsParser.add(o);
            }
            argsParser.parse(Arrays.asList(args));                              // throws ArgumentParsingError
        } catch (ArgumentParsingError e) {
            System.err.println(e.getMessage());
            System.err.println(argsParser.toUsageString());
            System.exit(1);
        }

        Level logLevel = Util.getLogLevelForNumber(Util.WARNING_LOG_LEVEL_NUM +
                                                   _verbosity);
        Util.setRootLogLevel(logLevel);

        ExecutorService executor =
            Executors.newFixedThreadPool(_numThreads);

        try (ServerSocketChannel listenSock = ServerSocketChannel.open()) {     // throws IOException

            listenSock.setOption(StandardSocketOptions.SO_REUSEADDR, true);     // throws IOException
            listenSock.bind(new InetSocketAddress(_address, _port));            // bind throws IOException

            while (true) {
                try {
                    final SocketChannel sock = listenSock.accept();             // throws IOException
                    // FIXME: do reverse name lookup before continuing
                    // TODO: re-enable socket timeout if not debugging
                    //_peerChannel.setSoTimeout(60);
                    // TODO: set TCP keep alive
                    InetAddress address = ((InetSocketAddress) sock.getRemoteAddress()).getAddress(); // getRemoteAddress and getAddress may both be null
                    Principal principal = new AddressPrincipal(address);        // throws IllegalArgumentException if address == null
                    Modules modules = moduleProvider.newInstance(principal);
                    Callable<Boolean> c = createCallable(executor, sock,
                                                         modules);
                    executor.submit(c);                                         // NOTE: result discarded
                } catch (ModuleException e) {
                    System.err.format("Error: failed to initialise modules " +
                                      "for principal %s using ModuleProvider " +
                                      "%s: %s%n", moduleProvider, e);
                }
            }
        } finally {
            System.err.println("shutting down");
            executor.shutdown();
            moduleProvider.close();
            while (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                System.err.println("some sessions are still running, waiting " +
                                   "for them to finish before exiting");
            }
        }
    }

    public static void main(String[] args)
        throws IOException, InterruptedException
    {
        System.err.println("Warning: this software is still unstable and " +
                           "there might be data corruption bugs hiding. So " +
                           "use it only carefully at your own risk.");

        new YajSyncServer().start(args);
    }
}
