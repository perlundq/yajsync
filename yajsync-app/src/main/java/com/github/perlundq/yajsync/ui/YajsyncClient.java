/*
 * A simple rsync command line client implementation
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013-2016 Per Lundqvist
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

import java.io.BufferedReader;
import java.io.Console;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.AuthProvider;
import com.github.perlundq.yajsync.FileSelection;
import com.github.perlundq.yajsync.RsyncClient;
import com.github.perlundq.yajsync.RsyncException;
import com.github.perlundq.yajsync.RsyncServer;
import com.github.perlundq.yajsync.Statistics;
import com.github.perlundq.yajsync.attr.DeviceInfo;
import com.github.perlundq.yajsync.attr.FileInfo;
import com.github.perlundq.yajsync.attr.Group;
import com.github.perlundq.yajsync.attr.RsyncFileAttributes;
import com.github.perlundq.yajsync.attr.SymlinkInfo;
import com.github.perlundq.yajsync.attr.User;
import com.github.perlundq.yajsync.internal.channels.ChannelException;
import com.github.perlundq.yajsync.internal.session.ChecksumHash;
import com.github.perlundq.yajsync.internal.session.FileAttributeManager;
import com.github.perlundq.yajsync.internal.session.FileAttributeManagerFactory;
import com.github.perlundq.yajsync.internal.session.SessionStatistics;
import com.github.perlundq.yajsync.internal.util.ArgumentParser;
import com.github.perlundq.yajsync.internal.util.ArgumentParsingError;
import com.github.perlundq.yajsync.internal.util.Environment;
import com.github.perlundq.yajsync.internal.util.FileOps;
import com.github.perlundq.yajsync.internal.util.Option;
import com.github.perlundq.yajsync.internal.util.Pair;
import com.github.perlundq.yajsync.internal.util.PathOps;
import com.github.perlundq.yajsync.internal.util.Triple;
import com.github.perlundq.yajsync.internal.util.Util;
import com.github.perlundq.yajsync.net.ChannelFactory;
import com.github.perlundq.yajsync.net.DuplexByteChannel;
import com.github.perlundq.yajsync.net.SSLChannelFactory;
import com.github.perlundq.yajsync.net.StandardChannelFactory;

public class YajsyncClient
{
    private static final int PORT_UNDEFINED = -1;

    private enum Mode
    {
        LOCAL_COPY, LOCAL_LIST, REMOTE_SEND, REMOTE_RECEIVE, REMOTE_LIST;

        public boolean isRemote()
        {
            return this == REMOTE_SEND || this == REMOTE_RECEIVE ||
                   this == REMOTE_LIST;
        }
    }

    private static final Logger _log =
        Logger.getLogger(YajsyncClient.class.getName());

    private final AuthProvider _authProvider = new AuthProvider()
    {
        @Override
        public String getUser()
        {
            return _userName;
        }

        @Override
        public char[] getPassword() throws IOException
        {
            if (_passwordFile != null) {
                if (!_passwordFile.equals("-")) {
                    Path p = Paths.get(_passwordFile);
                    FileAttributeManager fileManager =
                            FileAttributeManagerFactory.newMostAble(p.getFileSystem(),
                                                                    User.NOBODY, Group.NOBODY,
                                                                    Environment.DEFAULT_FILE_PERMS,
                                                                    Environment.DEFAULT_DIR_PERMS);
                    RsyncFileAttributes attrs = fileManager.stat(p);
                    if ((attrs.mode() & (FileOps.S_IROTH | FileOps.S_IWOTH)) != 0) {
                        throw new IOException(String.format(
                                "insecure permissions on %s: %s",
                                _passwordFile, attrs));
                    }
                }
                try (BufferedReader br = new BufferedReader(
                        _passwordFile.equals("-")
                        ? new InputStreamReader(System.in)
                        : new FileReader(_passwordFile))) {
                    return br.readLine().toCharArray();
                }
            }

            String passwordStr = Environment.getRsyncPasswordOrNull();
            if (passwordStr != null) {
                return passwordStr.toCharArray();
            }

            Console console = System.console();
            if (console == null) {
                throw new IOException("no console available");
            }
            return console.readPassword("Password: ");
        }
    };
    private final SimpleDateFormat _timeFormatter =
            new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private boolean _isShowStatistics;
    private boolean _isTLS;
    private boolean _readStdin = false;
    private FileSelection _fileSelection;
    private FileSystem _fs = FileSystems.getDefault();
    private int _contimeout = 0;
    private int _timeout = 0;
    private int _remotePort = PORT_UNDEFINED;
    private int _verbosity;
    private Path _cwd;
    private PrintStream _stderr = System.out;
    private PrintStream _stdout = System.out;
    private final RsyncClient.Builder _clientBuilder =
            new RsyncClient.Builder().authProvider(_authProvider);
    private Statistics _statistics = new SessionStatistics();
    private String _cwdName = Environment.getWorkingDirectoryName();
    private String _passwordFile;
    private String _userName;


    public YajsyncClient setStandardOut(PrintStream out)
    {
        _stdout = out;
        return this;
    }

    public YajsyncClient setStandardErr(PrintStream err)
    {
        _stderr = err;
        _clientBuilder.stderr(_stderr);
        return this;
    }

    public Statistics statistics()
    {
        return _statistics;
    }

    private List<Option> options()
    {
        List<Option> options = new LinkedList<>();

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "charset", "",
                                           "which charset to use (default UTF-8)",
                                           option -> {
                                               String charsetName = (String) option.getValue();
                                               try {
                                                   Charset charset = Charset.forName(charsetName);
                                                   _clientBuilder.charset(charset);
                                                   return ArgumentParser.Status.CONTINUE;
                                               } catch (IllegalCharsetNameException |
                                                        UnsupportedCharsetException e) {
                                                   throw new ArgumentParsingError(String.format(
                                                           "failed to set character set to %s: %s",
                                                           charsetName, e));
                                               }}));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "dirs", "d",
                                              "transfer directories without recursing (default " +
                                              "false unless listing files)",
                                              option -> {
                                                  if (_fileSelection == FileSelection.RECURSE) {
                                                      throw new ArgumentParsingError(
                                                              "--recursive and --dirs are " +
                                                              "incompatible options");
                                                  }
                                                  _fileSelection = FileSelection.TRANSFER_DIRS;
                                                  _clientBuilder.fileSelection(_fileSelection);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "recursive", "r",
                                              "recurse into directories (default false)",
                                              option -> {
                                                  if (_fileSelection == FileSelection.TRANSFER_DIRS)
                                                  {
                                                      throw new ArgumentParsingError(
                                                              "--recursive and --dirs are " +
                                                              "incompatible options");
                                                  }
                                                  _fileSelection = FileSelection.RECURSE;
                                                  _clientBuilder.fileSelection(_fileSelection);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "verbose", "v",
                                              "increase output verbosity (default quiet)",
                                              option -> {
                                                  _clientBuilder.verbosity(_verbosity++);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "devices", "",
                                              "_simulate_ preserve character device files and " +
                                              "block device files (default false)",
                                              option -> {
                                                  _clientBuilder.isPreserveDevices(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "specials", "",
                                              "_simulate_ preserve special device files - named " +
                                              "sockets and named pipes (default false)",
                                              option -> {
                                                  _clientBuilder.isPreserveSpecials(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "", "D",
                                              "same as --devices and --specials (default false)",
                                              option -> {
                                                  _clientBuilder.isPreserveDevices(true);
                                                  _clientBuilder.isPreserveSpecials(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "links", "l",
                                              "preserve symlinks (default false)",
                                              option -> {
                                                  _clientBuilder.isPreserveLinks(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "perms", "p",
                                              "preserve file permissions (default false)",
                                              option -> {
                                                  _clientBuilder.isPreservePermissions(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "times", "t",
                                              "preserve last modification time (default false)",
                                              option -> {
                                                  _clientBuilder.isPreserveTimes(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "owner", "o",
                                              "preserve owner (default false)",
                                              option -> {
                                                  _clientBuilder.isPreserveUser(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "group", "g",
                                              "preserve group (default false)",
                                              option -> {
                                                  _clientBuilder.isPreserveGroup(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "archive", "a",
                                              "archive mode - same as -rlptgoD (default false)",
                                              option -> {
                                                  _clientBuilder.
                                                      fileSelection(FileSelection.RECURSE).
                                                      isPreserveLinks(true).
                                                      isPreservePermissions(true).
                                                      isPreserveTimes(true).
                                                      isPreserveGroup(true).
                                                      isPreserveUser(true).
                                                      isPreserveDevices(true).
                                                      isPreserveSpecials(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "delete", "",
                                              "delete extraneous files (default false)",
                                              option -> {
                                                  _clientBuilder.isDelete(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "numeric-ids", "",
                                              "don't map uid/gid values by user/group name " +
                                              "(default false)",
                                              option -> {
                                                  _clientBuilder.isNumericIds(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "ignore-times", "I",
                                              "transfer files that match both size and time " +
                                              "(default false)",
                                              option -> {
                                                  _clientBuilder.isIgnoreTimes(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "stats", "",
                                              "show file transfer statistics",
                                              option -> {
                                                  _isShowStatistics = true;
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "password-file", "",
                                           "read daemon-access password from specified file " +
                                           "(where `-' is stdin)",
                                           option -> {
                                               _passwordFile = (String) option.getValue();
                                               return ArgumentParser.Status.CONTINUE;
                                           }));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL, "port", "",
                                            String.format("server port number (default %d)",
                                                          RsyncServer.DEFAULT_LISTEN_PORT),
                                            option -> {
                                                int port = (int) option.getValue();
                                                if (ConnInfo.isValidPortNumber(port)) {
                                                    _remotePort = port;
                                                    return ArgumentParser.Status.CONTINUE;
                                                } else {
                                                    throw new ArgumentParsingError(String.format(
                                                            "illegal port %d - must be within " +
                                                            "the range [%d, %d]", port,
                                                            ConnInfo.PORT_MIN, ConnInfo.PORT_MAX));
                                                }
                                            }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "stdin", "",
                                              "read list of source files from stdin",
                                              option -> {
                                                  _readStdin = true;
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        String deferredWriteHelp =
            "(receiver only) receiver defers writing into target tempfile as " +
            "long as possible to possibly eliminate all I/O writes for " +
            "identical files. This comes at the cost of a highly increased " +
            "risk of the file being modified by a process already having it " +
            "opened (default false)";

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "defer-write", "",
                                              deferredWriteHelp,
                                              option -> {
                                                  _clientBuilder.isDeferWrite(true);
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL, "timeout", "",
                                            "set I/O read timeout in seconds (default 0 - " +
                                            "disabled)",
                                            option -> {
                                                int timeout = (int) option.getValue();
                                                if (timeout < 0) {
                                                    throw new ArgumentParsingError(String.format(
                                                            "invalid timeout %d - mut be greater " +
                                                            "than or equal to 0", timeout));
                                                }
                                                _timeout = timeout * 1000;
                                                // Timeout socket operations depend on
                                                // ByteBuffer.array and ByteBuffer.arrayOffset.
                                                // Disable direct allocation if the resulting
                                                // ByteBuffer won't have an array.
                                                if (timeout > 0 &&
                                                    !Environment.hasAllocateDirectArray())
                                                {
                                                    Environment.setAllocateDirect(false);
                                                }
                                                return ArgumentParser.Status.CONTINUE;
                                            }));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL, "block-size", "B", "force a fixed checksum block-size",
                        option -> {
                            try {
                                _clientBuilder.blockSize( (int) option.getValue() );
                                return ArgumentParser.Status.CONTINUE;
                            } catch (IllegalArgumentException e) {
                                throw new ArgumentParsingError(e);
                            }
                        }));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL, "contimeout", "",
                                            "set daemon connection timeout in seconds (default " +
                                            "0 - disabled)",
                                            option -> {
                                                int contimeout = (int) option.getValue();
                                                if (contimeout >= 0) {
                                                    _contimeout = contimeout * 1000;
                                                    return ArgumentParser.Status.CONTINUE;
                                                } else {
                                                    throw new ArgumentParsingError(String.format(
                                                            "invalid connection timeout %d - " +
                                                            "must be greater than or equal to 0",
                                                            contimeout));
                                                }
                                            }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "tls", "",
                                              String.format("tunnel all data over TLS/SSL " +
                                                            "(default %s)", _isTLS),
                                              option -> {
                                                  _isTLS = true;
                                                  // SSLChannel.read and SSLChannel.write depends on
                                                  // ByteBuffer.array and ByteBuffer.arrayOffset.
                                                  // Disable direct allocation if the resulting
                                                  // ByteBuffer won't have an array.
                                                  if (!Environment.hasAllocateDirectArray()) {
                                                      Environment.setAllocateDirect(false);
                                                  }
                                                  return ArgumentParser.Status.CONTINUE;
                                              }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "cwd", "",
                                           "change current working directory (usable in " +
                                           "combination with --fs)",
                                           option -> {
                                               _cwdName = (String) option.getValue();
                                               return ArgumentParser.Status.CONTINUE;
                                           }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "fs", "",
                                           "use a non-default Java nio FileSystem implementation " +
                                           "(see also --cwd)",
                                           option -> {
                                               try {
                                                   String fsName = (String) option.getValue();
                                                   _fs = PathOps.fileSystemOf(fsName);
                                                   _cwdName = Util.firstOf(_fs.getRootDirectories()).toString();
                                                   return ArgumentParser.Status.CONTINUE;
                                               } catch (IOException | URISyntaxException e) {
                                                   throw new ArgumentParsingError(e);
                                               }
                                           }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "checksum-choice", "c",
                        "which hash to use for checksum ( md5, xxhash )",
                        option -> {
                            String checksumName = (String) option.getValue();
                            try {
                                _clientBuilder.checksumHash( ChecksumHash.valueOf( checksumName ));
                                return ArgumentParser.Status.CONTINUE;
                            } catch (IllegalArgumentException e) {
                                throw new ArgumentParsingError(String.format(
                                        "failed to set checksum hash to %s: %s",
                                        checksumName, e));
                            }}));
        return options;
    }

    private Triple<Mode, RsyncUrls, RsyncUrl>
    parseUnnamedArgs(List<String> unnamed) throws ArgumentParsingError
    {
        try {
            int len = unnamed.size();
            if (len == 0) {
                throw new ArgumentParsingError("Please specify at least one " +
                                               "non-option argument for (one " +
                                               "or more) source files and " +
                                               "optionally one destination " +
                                               "(defaults to current " +
                                               "directory)");
            }
            int numSrcArgs = len == 1 ? 1 : len - 1;
            List<String> srcFileNames = unnamed.subList(0, numSrcArgs);
            RsyncUrls srcUrls = new RsyncUrls(_cwd, srcFileNames);
            if (len == 1) {
                if (srcUrls.isRemote()) {
                    return new Triple<>(Mode.REMOTE_LIST, srcUrls, null);
                }
                return new Triple<>(Mode.LOCAL_LIST, srcUrls, null);
            }

            int indexOfLast = len - 1;
            String dstFileName = unnamed.get(indexOfLast);
            RsyncUrl dstUrl = RsyncUrl.parse(_cwd, dstFileName);
            if (srcUrls.isRemote() && dstUrl.isRemote()) {
                throw new ArgumentParsingError(String.format(
                        "source arguments %s and destination argument %s must" +
                        " not both be remote", srcUrls, dstUrl));
            } else if (srcUrls.isRemote()) {
                return new Triple<>(Mode.REMOTE_RECEIVE, srcUrls, dstUrl);
            } else if (dstUrl.isRemote()) {
                return new Triple<>(Mode.REMOTE_SEND, srcUrls, dstUrl);
            } else {
                return new Triple<>(Mode.LOCAL_COPY, srcUrls, dstUrl);
            }
        } catch (IllegalUrlException e) {
            throw new ArgumentParsingError(e);
        }
    }

    private void showStatistics(Statistics stats)
    {
        _stdout.format("Number of files: %d%n" +
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
            stats.totalBytesWritten(),
            stats.totalBytesRead());
    }

    private static List<String> readLinesFromStdin() throws IOException
    {
        List<String> lines = new LinkedList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                String line = br.readLine();
                if (line == null) {
                    return lines;
                }
                lines.add(line);
            }
        }
    }

    private Iterable<Path> getPaths(Iterable<String> pathNames)
    {
        List<Path> paths = new LinkedList<>();
        for (String pathName : pathNames) {
            Path p = PathOps.get(_cwd.getFileSystem(), pathName);
            paths.add(p);
        }
        return paths;
    }

    private RsyncClient.Result remoteTransfer(Mode mode,
                                              RsyncUrls srcArgs,
                                              RsyncUrl dstArgOrNull)
            throws RsyncException, InterruptedException
    {
        ConnInfo connInfo = srcArgs.isRemote()
                        ? srcArgs.connInfoOrNull()
                        : dstArgOrNull.connInfoOrNull();
        ChannelFactory socketFactory = _isTLS ? new SSLChannelFactory()
                                              : new StandardChannelFactory();

        if (_log.isLoggable(Level.FINE)) {
            _log.fine(String.format("connecting to %s (TLS=%b)",
                                    connInfo, _isTLS));
        }

        try (DuplexByteChannel sock = socketFactory.open(connInfo.address(),
                                                         connInfo.portNumber(),
                                                         _contimeout,
                                                         _timeout)) {  // throws IOException
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("connected to " + sock);
            }
            _userName = connInfo.userName();

            boolean isInterruptible = !_isTLS;
            RsyncClient.Remote client =
                    _clientBuilder.buildRemote(sock /* in */,
                                               sock /* out */,
                                               isInterruptible);
            switch (mode) {
            case REMOTE_SEND:
                return client.send(getPaths(srcArgs.pathNames())).
                              to(dstArgOrNull.moduleName(),
                                 dstArgOrNull.pathName());
            case REMOTE_RECEIVE:
                return client.receive(srcArgs.moduleName(),
                                      srcArgs.pathNames()).
                              to(PathOps.get(_cwd.getFileSystem(),
                                             dstArgOrNull.pathName()));
            case REMOTE_LIST:
                if (srcArgs.moduleName().isEmpty()) {
                    RsyncClient.ModuleListing listing = client.listModules();
                    while (true) {
                        String line = listing.take();
                        boolean isDone = line == null;
                        if (isDone) {
                            return listing.get();
                        }
                        System.out.println(line);
                    }
                } else {
                    RsyncClient.FileListing listing =
                        client.list(srcArgs.moduleName(),
                                    srcArgs.pathNames());
                    while (true) {
                        FileInfo f = listing.take();
                        boolean isDone = f == null;
                        if (isDone) {
                            return listing.get();
                        }
                        String ls = fileInfoToListingString(f);
                        System.out.println(ls);
                    }
                }
            default:
                throw new AssertionError(mode);
            }
        } catch (UnknownHostException | UnresolvedAddressException e) {
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe(String.format("Error: failed to resolve %s (%s)",
                                          connInfo.address(), e.getMessage()));
            }
        } catch (IOException e) { // SocketChannel.{open,close}()
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe("Error: socket open/close error: " +
                            e.getMessage());
            }
        } catch (ChannelException e) {
            if (_log.isLoggable(Level.SEVERE)) {
                _log.log(Level.SEVERE,
                        "Error: communication closed with peer: ", e);
            }
        }
        return RsyncClient.Result.failure();
    }

    public int start(String[] args)
    {
        ArgumentParser argsParser =
                ArgumentParser.newWithUnnamed(getClass().getSimpleName(),
                                              "files...");
        argsParser.addHelpTextDestination(_stdout);

        try {
            for (Option o : options()) {
                argsParser.add(o);
            }
            ArgumentParser.Status rc = argsParser.parse(Arrays.asList(args));
            if (rc != ArgumentParser.Status.CONTINUE) {
                return rc == ArgumentParser.Status.EXIT_OK ? 0 : 1;
            }
            _cwd = _fs.getPath(_cwdName);

            List<String> unnamed = new LinkedList<>();
            if (_readStdin) {
                unnamed.addAll(readLinesFromStdin());
            }
            unnamed.addAll(argsParser.getUnnamedArguments());

            Triple<Mode, RsyncUrls, RsyncUrl> res = parseUnnamedArgs(unnamed);
            Mode mode = res.first();
            RsyncUrls srcArgs = res.second();
            RsyncUrl dstArgOrNull = res.third();

            if (_remotePort != PORT_UNDEFINED && mode.isRemote()) {
                Pair<RsyncUrls, RsyncUrl> res2 = updateRemotePort(_cwd,
                                                                  _remotePort,
                                                                  srcArgs,
                                                                  dstArgOrNull);
                srcArgs = res2.first();
                dstArgOrNull = res2.second();
            }

            Level logLevel = Util.getLogLevelForNumber(
                    Util.WARNING_LOG_LEVEL_NUM + _verbosity);
            Util.setRootLogLevel(logLevel);
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("%s src: %s, dst: %s",
                                        mode, srcArgs, dstArgOrNull));
            }

            RsyncClient.Result result;
            if (mode.isRemote()) {
                result = remoteTransfer(mode, srcArgs, dstArgOrNull);
            } else if (mode == Mode.LOCAL_COPY) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("starting local transfer (using rsync's delta " +
                              "transfer algorithm - i.e. will not run with a " +
                              "--whole-file option, so performance is most " +
                              "probably lower than rsync)");
                }
                result = _clientBuilder.buildLocal().
                                            copy(getPaths(srcArgs.pathNames())).
                                            to(PathOps.get(_cwd.getFileSystem(),
                                                           dstArgOrNull.pathName()));
            } else if (mode == Mode.LOCAL_LIST) {
                RsyncClient.FileListing listing = _clientBuilder.
                        buildLocal().
                        list(getPaths(srcArgs.pathNames()));
                while (true) {
                    FileInfo f = listing.take();
                    boolean isDone = f == null;
                    if (isDone) {
                        result = listing.get();
                        break;
                    }
                    System.out.println(fileInfoToListingString(f));
                }
            } else {
                throw new AssertionError();
            }
            _statistics = result.statistics();
            if (_isShowStatistics) {
                showStatistics(result.statistics());
            }
            if (_log.isLoggable(Level.INFO)) {
                _log.info("exit status: " + (result.isOK() ? "OK" : "ERROR"));
            }
            return result.isOK() ? 0 : -1;

        } catch (ArgumentParsingError e) {
            _stderr.println(e.getMessage());
            _stderr.println(argsParser.toUsageString());
        } catch (IOException e) { // reading from stdinp
            _stderr.println(e.getMessage());
        } catch (RsyncException e) {
            if (_log.isLoggable(Level.SEVERE)) {
                _log.severe(e.getMessage());
            }
        } catch (InterruptedException e) {
            if (_log.isLoggable(Level.SEVERE)) {
                _log.log(Level.SEVERE, "", e);
            }
        }
        return -1;
    }

    private static Pair<RsyncUrls, RsyncUrl>
    updateRemotePort(Path cwd, int newPortNumber, RsyncUrls srcArgs,
                     RsyncUrl dstArgOrNull)
            throws ArgumentParsingError
    {
        try {
            ConnInfo connInfo = srcArgs.isRemote()
                    ? srcArgs.connInfoOrNull() : dstArgOrNull.connInfoOrNull();
            // Note: won't detect ambiguous ports if explicitly specifying 873
            // in rsync:// url + something else in --port=
            if (connInfo.portNumber() != RsyncServer.DEFAULT_LISTEN_PORT &&
                newPortNumber != connInfo.portNumber())
            {
                throw new ArgumentParsingError(String.format(
                        "ambiguous remote ports: %d != %d", newPortNumber,
                        connInfo.portNumber()));
            }
            ConnInfo newConnInfo = new ConnInfo.Builder(connInfo.address()).
                                          portNumber(newPortNumber).
                                          userName(connInfo.userName()).build();
            if (srcArgs.isRemote()) {
                return new Pair<>(new RsyncUrls(newConnInfo,
                                                srcArgs.moduleName(),
                                                srcArgs.pathNames()),
                                  dstArgOrNull);
            } // else if (dstArg.isRemote()) {
            return new Pair<>(srcArgs,
                              new RsyncUrl(cwd, newConnInfo,
                                           dstArgOrNull.moduleName(),
                                           dstArgOrNull.pathName()));
        } catch (IllegalUrlException e) {
            throw new RuntimeException(e);
        }
    }

    private String fileInfoToListingString(FileInfo f)
    {
        RsyncFileAttributes attrs = f.attrs();
        Date t = new Date(FileTime.from(attrs.lastModifiedTime(),
                                        TimeUnit.SECONDS).toMillis());
        if (f instanceof SymlinkInfo) {
            return String.format("%s %11d %s %s -> %s",
                                 FileOps.modeToString(attrs.mode()),
                                 attrs.size(),
                                 _timeFormatter.format(t),
                                 f.pathName(),
                                 ((SymlinkInfo) f).targetPathName());
        } else if (f instanceof DeviceInfo) {
            DeviceInfo d = (DeviceInfo) f;
            return String.format("%s %11d %d,%d %s %s",
                                 FileOps.modeToString(attrs.mode()),
                                 attrs.size(),
                                 d.major(),
                                 d.minor(),
                                 _timeFormatter.format(t),
                                 d.pathName());
        }
        return String.format("%s %11d %s %s",
                             FileOps.modeToString(attrs.mode()),
                             attrs.size(),
                             _timeFormatter.format(t),
                             f.pathName());
    }
}
