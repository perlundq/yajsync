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
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.session.RsyncServerSession;
import com.github.perlundq.yajsync.session.Statistics;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.ArgumentParser;
import com.github.perlundq.yajsync.util.ArgumentParsingError;
import com.github.perlundq.yajsync.util.Consts;
import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.Option;
import com.github.perlundq.yajsync.util.Util;

public class YajSyncServer
{
    private static final Logger _log =
        Logger.getLogger(YajSyncServer.class.getName());

    private final static int THREAD_FACTOR = 4;
    private final String _appName = getClass().getSimpleName();
    private final String _defaultConfigFile = _appName + ".conf";

    private boolean _isDeferredWrite;
    private Charset _charset = Charset.forName(Text.UTF8_NAME);
    private Configuration _configuration;
    private int _numThreads = Runtime.getRuntime().availableProcessors() *
                              THREAD_FACTOR;
    private InetAddress _address = InetAddress.getLoopbackAddress(); 
    private int _port = Consts.DEFAULT_LISTEN_PORT;
    private int _verbosity;
    private String _cfgFileName =
        Environment.getServerConfig(_defaultConfigFile);

    public YajSyncServer() {}

    public void parseArgs(String[] args) {
        ArgumentParser argsParser = ArgumentParser.newNoUnnamed(_appName);

        argsParser.addHelpTextDestination(System.out);

        argsParser.add(
            Option.newStringOption(Option.Policy.OPTIONAL,
                                   "charset", "",
                                   String.format("which charset to use " +
                                                 "(default %s)",
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

        argsParser.add(
            Option.newStringOption(Option.Policy.OPTIONAL, "config", "",
                                   String.format("path to config file " +
                                                 "(default %s)",
                                                 _cfgFileName),
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _cfgFileName = (String) option.getValue();
                }}));

        argsParser.add(
            Option.newWithoutArgument(Option.Policy.OPTIONAL,
                                      "verbose", "v",
                                      String.format("output verbosity " +
                                                    "(default %d)",
                                                    _verbosity),
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _verbosity++;
                }}));
        
        argsParser.add(
            Option.newStringOption(Option.Policy.OPTIONAL, "address", "",
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

        argsParser.add(
            Option.newIntegerOption(Option.Policy.OPTIONAL,
                                    "port", "",
                                    String.format("port number to listen on " +
                                                  "(default %d)", _port),
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _port = (int) option.getValue();
                }}));

        argsParser.add(
            Option.newIntegerOption(Option.Policy.OPTIONAL,
                                    "threads", "",
                                    String.format("size of thread pool " +
                                                  "(default %d)",
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
        argsParser.add(
            Option.newWithoutArgument(Option.Policy.OPTIONAL,
                                      "defer-write", "",
                                      deferredWriteHelp,
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _isDeferredWrite = true;
                }}));

        try {
            argsParser.parse(Arrays.asList(args));
        } catch (ArgumentParsingError e) {
            System.err.println(e.getMessage());
            System.err.println(argsParser.toUsageString());
            System.exit(1);
        }
    }

    private Callable<Boolean> createCallable(final ExecutorService executor,
                                             final SocketChannel sock,
                                             final Map<String, Module> modules)
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
                    isOK = session.startSession(executor, sock, sock, modules);
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

    private void sessionCreatorLoop(ExecutorService executor,
                                    ServerSocketChannel listenSock)
        throws IOException
    {
        assert _configuration != null;
        while (true) {
            @SuppressWarnings("resource") // closed from within returned t
            final SocketChannel sock = listenSock.accept();
            // FIXME: do reverse name lookup before continuing
            // TODO: re-enable socket timeout if not debugging
            //_peerChannel.setSoTimeout(60);
            // TODO: set TCP keep alive
            try {
                _configuration = Configuration.readFile(_cfgFileName);
            } catch (IOException e) {
                System.err.format("Warning: Failed to re-read " +
                    "configuration file %s: %s%n",
                    _cfgFileName, e.getMessage());
            }
            Callable<Boolean> t = createCallable(executor, sock,
                                                 _configuration.modules());
            executor.submit(t);  // NOTE: result discarded
        }
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

    public static void main(String[] args)
        throws IOException, InterruptedException
    {
        System.err.println("Warning: this software is still unstable and " +
                           "there might be data corruption bugs hiding. So " +
                           "use it only carefully at your own risk.");

        final YajSyncServer server = new YajSyncServer();
        server.parseArgs(args);
        Level logLevel = Util.getLogLevelForNumber(Util.WARNING_LOG_LEVEL_NUM +
                                                   server._verbosity);
        Util.setRootLogLevel(logLevel);

        try {
            server._configuration = Configuration.readFile(server._cfgFileName);
        } catch (IOException e) { // 
            System.err.format("Error: failed to read configuration file " +
                              "%s (%s)%n", server._cfgFileName, e);
            System.exit(1);
        }

        ExecutorService executor =
            Executors.newFixedThreadPool(server._numThreads);

        try (ServerSocketChannel listenSock = ServerSocketChannel.open()) {
            listenSock.setOption(StandardSocketOptions.SO_REUSEADDR, true);

            InetSocketAddress socketAddress =
                new InetSocketAddress(server._address, server._port);
            listenSock.bind(socketAddress);
            server.sessionCreatorLoop(executor, listenSock);
        } finally {
            System.err.format("shutting down%n");
            executor.shutdown();
            while (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                System.err.format("some sessions are still running, waiting " +
                                  "for them to finish before exiting");
            }
        }
    }
}
