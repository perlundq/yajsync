/*
 * A simple rsync command line client implementation
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

import java.io.Console;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.channels.net.ChannelFactory;
import com.github.perlundq.yajsync.channels.net.DuplexByteChannel;
import com.github.perlundq.yajsync.channels.net.SSLChannelFactory;
import com.github.perlundq.yajsync.channels.net.StandardChannelFactory;
import com.github.perlundq.yajsync.session.ClientSessionConfig;
import com.github.perlundq.yajsync.session.RsyncClientSession;
import com.github.perlundq.yajsync.session.RsyncException;
import com.github.perlundq.yajsync.session.RsyncLocal;
import com.github.perlundq.yajsync.session.Statistics;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.ArgumentParser;
import com.github.perlundq.yajsync.util.ArgumentParsingError;
import com.github.perlundq.yajsync.util.Consts;
import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.Option;
import com.github.perlundq.yajsync.util.PathOps;
import com.github.perlundq.yajsync.util.Util;

public class YajSyncClient implements ClientSessionConfig.AuthProvider
{
    private enum ArgType { LOCAL, REMOTE }

    private static class Argument {
        /*
         [USER@]HOST::SRC...
         rsync://[USER@]HOST[:PORT]/SRC
         */
        private static final int PORT_UNDEFINED = -1;
        private static final int PORT_MIN = 1;
        private static final int PORT_MAX = 65535;
        private static final String USER_REGEX = "[^@: ]+@";
        private static final String HOST_REGEX = "[^:/]+";
        private static final String PORT_REGEX = ":[0-9]+";
        private static final String MODULE_REGEX = "[^/]+";
        private static final String PATH_REGEX = "/.*";
        private static final Pattern MODULE =
            Pattern.compile(String.format("^(%s)?(%s)::(%s)?(%s)?$",
                                          USER_REGEX, HOST_REGEX,
                                          MODULE_REGEX, PATH_REGEX));
        private static final Pattern URL =
            Pattern.compile(String.format("^rsync://(%s)?(%s)(%s)?(/%s)?(%s)?$",
                                          USER_REGEX, HOST_REGEX, PORT_REGEX,
                                          MODULE_REGEX, PATH_REGEX));
        private final String _userName;
        private final String _address;
        private final int _port;
        private final String _moduleName;
        private final String _pathName;

        private Argument(String userName, String address, String moduleName,
                         int port, String pathName)
            throws ArgumentParsingError
        {
            assert userName != null;
            assert address != null;
            assert moduleName != null;
            assert pathName != null;
            assert userName.isEmpty() || !address.isEmpty();

            if (address.isEmpty() && port != PORT_UNDEFINED) {
                throw new ArgumentParsingError(String.format(
                    "port %d specified without an address", port));
            }
            if (port != PORT_UNDEFINED &&
                (port < PORT_MIN || port > PORT_MAX)) {
                throw new ArgumentParsingError(
                    String.format("illegal port %d - must be within the range" +
                                  " [%d, %d]", PORT_MIN, PORT_MAX));
            }
            if (!address.isEmpty() &&
                moduleName.isEmpty() && !pathName.isEmpty()) {
                throw new ArgumentParsingError(String.format(
                    "remote path %s specified without a module", pathName));
            }

            _userName = userName;
            _address = address;
            _port = port;
            _moduleName = moduleName;

            if (address.isEmpty()) {
                assert userName.isEmpty() && moduleName.isEmpty() &&
                       port == PORT_UNDEFINED;
                _pathName = toLocalPathName(pathName);
            } else {
                _pathName = toRemotePathName(moduleName, pathName);
            }
        }

        @Override
        public String toString()
        {
            return String.format("%s (userName=%s, address=%s, moduleName=%s," +
                                 " port=%d, path=%s)",
                                 getClass().getSimpleName(), _userName,
                                 _address, _moduleName, _port, _pathName);
        }

        private boolean isSameUserHostPortAs(Argument otherArg)
        {
            assert otherArg != null;
            return _address.equals(otherArg._address) &&
                   _port == otherArg._port &&
                   _userName.equals(otherArg._userName);
        }

        private static Argument matchModule(String arg)
            throws ArgumentParsingError
        {
            Matcher mod = MODULE.matcher(arg);
            if (!mod.matches()) {
                return null;
            }
            String userName = Text.stripLast(Text.nullToEmptyStr(mod.group(1)));
            String address = mod.group(2);
            String moduleName = Text.nullToEmptyStr(mod.group(3));
            int port = PORT_UNDEFINED;
            String path = Text.nullToEmptyStr(mod.group(4));
            return new Argument(userName, address, moduleName, port, path);
        }

        private static Argument matchURL(String arg) throws ArgumentParsingError
        {
            Matcher url = URL.matcher(arg);
            if (!url.matches()) {
                return null;
            }
            String userName = Text.stripLast(Text.nullToEmptyStr(url.group(1)));
            String address = url.group(2);
            String moduleName = Text.stripFirst(Text.nullToEmptyStr(url.group(4)));
            int port = url.group(3) == null
                             ? PORT_UNDEFINED
                             : Integer.parseInt(Text.stripFirst(url.group(3)));
            String path = Text.nullToEmptyStr(url.group(5));
            return new Argument(userName, address, moduleName, port, path);
        }

        private static Argument parse(String arg) throws ArgumentParsingError
        {
            assert arg != null;

            if (arg.isEmpty()) {
                throw new ArgumentParsingError("empty string");
            }

            Argument result = matchModule(arg);
            if (result != null) {
                return result;
            }
            result = matchURL(arg);
            if (result != null) {
                return result;
            }
            return new Argument("", "", "", PORT_UNDEFINED, arg);
        }

        private ArgType type()
        {
            if (_address.isEmpty()) {
                return ArgType.LOCAL;
            }
            return ArgType.REMOTE;
        }

        private static String toLocalPathName(String pathName)
        {
            assert !pathName.isEmpty();
            Path p = Paths.get(pathName);
            if (pathName.endsWith(Text.SLASH)) {
                p = p.resolve(PathOps.DOT_DIR);  // Paths.get returns normalized path, any trailing slash is not preserved
            }
            if (p.isAbsolute()) {
                return p.toString();
            }
            return Environment.getWorkingDirectory().resolve(p).toString();
        }

        private static String toRemotePathName(String moduleName,
                                               String pathName)
        {
            if (moduleName.isEmpty() && pathName.isEmpty()) {
                return "";
            }
            if (pathName.isEmpty()) {
                return moduleName + Text.SLASH;
            }
            return moduleName + pathName;
        }
    }

    private static final Logger _log =
        Logger.getLogger(YajSyncClient.class.getName());

    private boolean _isDeferredWrite;
    private boolean _isModuleListing;
    private boolean _isPreserveTimes;
    private boolean _isRecursiveTransfer;
    private boolean _isRemote;
    private boolean _isSender;
    private boolean _isShowStatistics;
    private int _remotePort = Consts.DEFAULT_LISTEN_PORT;
    private int _verbosity = 0;
    private List<String> _srcArgs = new LinkedList<>();
    private Statistics _statistics;
    private String _address;
    private Charset _charset = Charset.forName(Text.UTF8_NAME);
    private String _dstArg;
    private String _moduleName;
    private String _userName;
    private boolean _isTLS;

    @Override
    public String getUser()
    {
        return _userName;
    }

    // TODO: add support for reading password from other sources than the console
    @Override
    public char[] getPassword()
    {
        Console console = System.console();
        if (console == null) {
            return "".toCharArray();
        }
        char[] password = console.readPassword("Password: ");
        return password;
    }

    private Iterable<Option> options()
    {
        List<Option> options = new LinkedList<>();
        options.add(
            Option.newStringOption(Option.Policy.OPTIONAL,
                                   "charset", "",
                                   String.format("which charset to use " +
                                                 "(default %s)",
                                                 _charset),
            new Option.Handler() {
                @Override public void handle(Option option)
                    throws ArgumentParsingError {
                    String charsetName = (String) option.getValue();
                    try {
                        _charset = Charset.forName(charsetName);
                    } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
                        throw new ArgumentParsingError(
                            String.format("failed to set character set to %s: %s",
                                charsetName, e));
                    }
                    if (!Util.isValidCharset(_charset)) {
                        throw new ArgumentParsingError(String.format(
                            "character set %s is not supported - cannot " +
                            "encode SLASH (/), DOT (.), NEWLINE (\n), " +
                            "CARRIAGE RETURN (\r) and NULL (\0) to their " +
                            "ASCII counterparts and vice versa", charsetName));
                    }
                }}));

        options.add(
            Option.newWithoutArgument(Option.Policy.OPTIONAL,
                                      "recursive", "r",
                                      String.format("recurse into directories" +
                                                    " (default %s)",
                                                    _isRecursiveTransfer),
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _isRecursiveTransfer  = true;
                }}));

        options.add(
            Option.newWithoutArgument(Option.Policy.OPTIONAL,
                                      "verbose", "v",
                                      String.format("output verbosity " +
                                                    "(default %d)",
                                                    _verbosity),
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _verbosity++;
                }}));

        options.add(
            Option.newWithoutArgument(Option.Policy.OPTIONAL,
                                      "times", "t",
                                      String.format("preserve last " +
                                                    "modification time " +
                                                    "(default %s)",
                                                    _isPreserveTimes),
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _isPreserveTimes  = true;
                }}));

        options.add(
            Option.newWithoutArgument(Option.Policy.OPTIONAL,
                                      "stats", "",
                                      "show file transfer statistics",
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _isShowStatistics = true;
                }}));

        options.add(
            Option.newIntegerOption(Option.Policy.OPTIONAL,
                                    "port", "",
                                    String.format("server port number " +
                                                  "(default %d)", _remotePort),
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _remotePort = (int) option.getValue();
                }}));

        String deferredWriteHelp = String.format(
            "(receiver only) receiver defers writing into target tempfile as long as possible" +
            " to possibly eliminate all I/O writes for identical files. This " +
            "comes at the cost of a highly increased risk of the file being " +
            "modified by a process already having it opened (default %s)",
            _isDeferredWrite);

        options.add(
            Option.newWithoutArgument(Option.Policy.OPTIONAL,
                                      "defer-write", "", deferredWriteHelp,
            new Option.Handler() {
                @Override public void handle(Option option) {
                    _isDeferredWrite = true;
                }}));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL,
                                              "tls", "",
                                              String.format("tunnel all data " +
                                                            "over TLS/SSL " +
                                                            "(default %s)",
                                                            _isTLS),
            new Option.Handler() {
            @Override public void handle(Option option) {
                _isTLS = true;
                // SSLChannel.read and SSLChannel.write depends on
                // ByteBuffer.array and ByteBuffer.arrayOffset. Disable direct
                // allocation if the resulting ByteBuffer won't have an array.
                if (!Environment.hasAllocateDirectArray()) {
                    Environment.setAllocateDirect(false);
                }
            }
        }));

        return options;
    }

    private void parseUnnamedArgs(List<String> unnamed)
        throws ArgumentParsingError
    {
        if (unnamed.size() == 0) {
            throw new ArgumentParsingError(
                "Please specify at least 1 non-option argument for (one or " +
                "more) source files and optionally one destination (defaults " +
                "to current directory)");
        }

        List<String> srcFileNames;
        String dstFileName;
        if (unnamed.size() == 1) {
            srcFileNames = unnamed.subList(0, 1);
            dstFileName = ".";
            _isModuleListing  = true;
        } else {
            srcFileNames = unnamed.subList(0, unnamed.size() - 1);
            dstFileName = unnamed.get(unnamed.size() - 1);
        }

        Argument prevArg = null;

        // FIXME: it's OK if remote arguments other than the first skip the
        // module name
        for (String argName : srcFileNames) {
            Argument arg = Argument.parse(argName);
            if (prevArg != null && !arg.isSameUserHostPortAs(prevArg)) {
                throw new ArgumentParsingError(
                    String.format("remote source arguments %s and %s are " +
                                  "incompatble", prevArg, arg));
            }
            if (!arg._pathName.isEmpty()) {
                _srcArgs.add(arg._pathName);
            }
            prevArg = arg;
        }

        Argument lastSrcArg = prevArg;
        Argument dstArg = Argument.parse(dstFileName);
        if (lastSrcArg.type() == ArgType.REMOTE &&
            dstArg.type() == ArgType.REMOTE) { // lastSrcArg can't be null given the previous size check of unNamed
            throw new ArgumentParsingError(String.format(
                "source arguments %s and destination " +
                "argument %s must not both be " +
                "remote (%s vs %s)",
                srcFileNames, dstFileName, lastSrcArg, dstArg));
        }
        _isRemote = lastSrcArg.type() == ArgType.REMOTE ||
                    dstArg.type() == ArgType.REMOTE;
        if (dstArg._pathName.isEmpty()) {
            assert dstArg.type() == ArgType.REMOTE;
            throw new ArgumentParsingError(String.format(
                "illegal remote argument %s", dstArg));
        }

        _dstArg = dstArg._pathName;
        _isSender = dstArg.type() == ArgType.REMOTE;
        Argument remoteArg = _isSender ? dstArg : lastSrcArg;
        _moduleName = remoteArg._moduleName;
        _address = remoteArg._address;
        if (remoteArg._port != Argument.PORT_UNDEFINED) {
            _remotePort = remoteArg._port;
        }

        String userName = remoteArg._userName.isEmpty()
                              ? Environment.getUserName()
                              : remoteArg._userName;
        _userName = userName;
    }

    private void showStatistics(Statistics stats)
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

    public void start(String[] args)
    {
        ArgumentParser argsParser =
            ArgumentParser.newWithUnnamed(getClass().getSimpleName(),
                                          "files...");
        argsParser.addHelpTextDestination(System.out);
        try {
            for (Option o : options()) {
                argsParser.add(o);
            }
            argsParser.parse(Arrays.asList(args));                              // TODO: print warning for any not applicable option (e.g. defer-write for sender)
            parseUnnamedArgs(argsParser.getUnnamedArguments());
        } catch (ArgumentParsingError e) {
            System.err.println(e.getMessage());
            System.err.println(argsParser.toUsageString());
            System.exit(1);
        }

        Level logLevel = Util.getLogLevelForNumber(Util.WARNING_LOG_LEVEL_NUM +
                                                   _verbosity);
        Util.setRootLogLevel(logLevel);
        ExecutorService executor = Executors.newCachedThreadPool();

        try {
            boolean isOK;
            if (_isRemote ) {
                isOK = startRemoteSession(executor);
            } else {
                isOK = startLocalSession(executor);
            }
            if (_isShowStatistics) {
                showStatistics(_statistics);
            }
            if (_log.isLoggable(Level.INFO)) {
                _log.info("exit status: " + (isOK ? "OK" : "ERROR"));
            }
            if (!isOK) {
                System.exit(1);
            }
        } finally {
            executor.shutdown();
        }
    }

    private boolean startRemoteSession(ExecutorService executor)
    {
        if (_log.isLoggable(Level.FINE)) {
            _log.fine("starting remote session");
        }

        RsyncClientSession session = new RsyncClientSession();
        session.setCharset(_charset);
        session.setIsDeferredWrite(_isDeferredWrite);
        session.setIsModuleListing(_isModuleListing);
        session.setIsPreserveTimes(_isPreserveTimes);
        session.setIsRecursiveTransfer(_isRecursiveTransfer);
        session.setIsSender(_isSender);

        ChannelFactory socketFactory = _isTLS ? new SSLChannelFactory()
                                              : new StandardChannelFactory();
        boolean isInterruptible = !_isTLS;

        try (DuplexByteChannel sock = socketFactory.open(_address,
                                                         _remotePort)) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("connected to " + sock);
            }
            return session.transfer(executor,
                                        sock,           // in
                                        sock,           // out
                                        _srcArgs,
                                        _dstArg,
                                        this,           // ClientSessionConfig.AuthProvider
                                        _moduleName,
                                        isInterruptible);
        } catch (UnknownHostException | UnresolvedAddressException e) {
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe(String.format("Error: failed to resolve %s (%s)",
                                          _address, e.getMessage()));
            }
        } catch (IOException e) { // SocketChannel.{open,close}()
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe("Error: socket open/close error: " +
                            e.getMessage());
            }
        } catch (ChannelException e) {              // startSession
            if (_log.isLoggable(Level.SEVERE)) {
                _log.log(Level.SEVERE,
                    "Error: communication closed with peer: ", e);
            }
        } catch (RsyncException e) {
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe(e.getMessage());
            }
        } catch (InterruptedException e) {                                      // should not happen
            if (_log.isLoggable(Level.SEVERE)) {
                _log.log(Level.SEVERE, "", e);
            }
        } finally {
            _statistics = session.statistics();
        }

        return false;
    }

    private boolean startLocalSession(ExecutorService executor)
    {
        if (_log.isLoggable(Level.FINE)) {
            _log.fine("starting local transfer (using rsync's delta " +
                      "transfer algorithm - i.e. will not run with a " +
                      "--whole-file option, so performance is most " +
                      "probably lower than rsync)");
        }

        RsyncLocal localTransfer = new RsyncLocal();
        localTransfer.setCharset(_charset);
        localTransfer.setVerbosity(_verbosity);
        localTransfer.setIsRecursiveTransfer(_isRecursiveTransfer);
        localTransfer.setIsPreserveTimes(_isPreserveTimes);
        localTransfer.setIsDeferredWrite(_isDeferredWrite);
        List<Path> srcPaths = new LinkedList<>();
        for (String pathName : _srcArgs) {
            srcPaths.add(Paths.get(pathName));                                  // throws InvalidPathException
        }

        try {
            return localTransfer.transfer(executor, srcPaths, _dstArg);
        } catch (ChannelException e) {
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe("Error: communication closed with peer: " +
                            e.getMessage());
            }
        } catch (RsyncException e) {
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe(e.getMessage());
            }
        } catch (InterruptedException e) {                                      // should not happen
            if (_log.isLoggable(Level.SEVERE)) {
                _log.log(Level.SEVERE, "", e);
            }
        } finally {
            _statistics = localTransfer.statistics();
        }
        return false;
    }

    public static void main(String[] args)
    {
        System.err.println("Warning: this software is still unstable and " +
                           "there might be data corruption bugs hiding. So " +
                           "use it only carefully at your own risk.");

        new YajSyncClient().start(args);
    }
}
