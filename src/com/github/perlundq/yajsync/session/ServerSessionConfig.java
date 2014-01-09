/*
 * Rsync server -> client handshaking protocol
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

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.channels.ChannelEOFException;
import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.text.TextConversionException;
import com.github.perlundq.yajsync.ui.Module;
import com.github.perlundq.yajsync.util.ArgumentParser;
import com.github.perlundq.yajsync.util.ArgumentParsingError;
import com.github.perlundq.yajsync.util.BitOps;
import com.github.perlundq.yajsync.util.Consts;
import com.github.perlundq.yajsync.util.MemoryPolicy;
import com.github.perlundq.yajsync.util.Option;
import com.github.perlundq.yajsync.util.OverflowException;
import com.github.perlundq.yajsync.util.PathOps;
import com.github.perlundq.yajsync.util.Util;

public class ServerSessionConfig extends SessionConfig
{
    private static final Logger _log =
        Logger.getLogger(ServerSessionConfig.class.getName());
    private final List<Path> _sourceFiles = new LinkedList<>();
    private Path _receiverDestination;
    private boolean _isIncrementalRecurse = false;
    private boolean _isRecursiveTransfer = false;
    private boolean _isSender = false;
    private boolean _isPreserveTimes = false;
    private Module _module;
    private int _verbosity = 0;

    /**
     * @throws IllegalArgumentException if charset is not supported
     */
    private ServerSessionConfig(SocketChannel peerConnection, Charset charset)
    {
        super(peerConnection, charset);
        int seedValue = (int) System.currentTimeMillis();
        _checksumSeed = BitOps.toLittleEndianBuf(seedValue);
    }

    /**
     * @throws IllegalArgumentException if charset is not supported
     * @throws RsyncProtocolException if failing to encode/decode characters
     *         correctly
     * @throws RsyncProtocolException if failed to parse arguments sent by peer
     *         correctly
     */
    public static ServerSessionConfig handshake(Charset charset,
                                                SocketChannel peerConnection,
                                                Map<String, Module> modules)
        throws ChannelException
    {
        assert peerConnection != null;
        assert modules!= null;

        ServerSessionConfig instance = new ServerSessionConfig(peerConnection,
                                                               charset);
        try {
            instance.exchangeProtocolVersion();
            String moduleName = instance.receiveModule();
            if (moduleName.isEmpty()) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("sending module listing and exiting");
                }
                instance.sendModuleListing(modules.values());
                instance.sendStatus(SessionStatus.EXIT);
                instance._status = SessionStatus.EXIT;
                return instance;
            }
            Module module = modules.get(moduleName);
            if (module == null) {
                if (_log.isLoggable(Level.WARNING)) {
                    _log.warning(String.format("Module %s does not exist",
                                               moduleName));
                }
                instance._status = SessionStatus.EXIT;
                return instance;
            }
            instance.setModule(module);
            instance.sendStatus(SessionStatus.OK);
            instance._status = SessionStatus.OK;

            Collection<String> args = instance.receiveArguments();
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("parsing arguments: " + args);
            }
            instance.parseArguments(args);
            instance.sendCompatibilities();
            instance.sendChecksumSeed();
            return instance;
        } catch (ArgumentParsingError | TextConversionException e) {
            throw new RsyncProtocolException(e);
        } finally {
            instance.flush();
        }
    }

    public int verbosity()
    {
        return _verbosity;
    }

    private void flush() throws ChannelException
    {
        _peerConnection.flush();
    }

    /**
     * @throws TextConversionException
     */
    private void sendModuleListing(Iterable<Module> modules)
        throws ChannelException
    {
        for (Module module: modules) {
            if (module.isGlobal()) {
                continue;
            }
            if (module.comment().isEmpty()) {
                writeString(String.format("%-15s\n", module.name()));
            } else {
                writeString(String.format("%-15s\t%s\n",
                                          module.name(),
                                          module.comment()));
            }
        }
    }

    private void sendStatus(SessionStatus status) throws ChannelException
    {
        writeString(status.toString() + "\n");
    }

    // TODO: authenticate module and possible execute
    //       protocolContext.sendStatus(ReplyStatus.AUTHREQ);
    private void setModule(Module module)
    {
        _module = module;
    }

    /**
     * @throws TextConversionException
     */
    private String readStringUntilNullOrEof() throws ChannelException
    {
        ByteBuffer buf = ByteBuffer.allocate(64);
        try {
            while (true) {
                byte b = _peerConnection.getByte();
                if (b == Text.ASCII_NULL) {
                    break;
                } else if (!buf.hasRemaining()) {
                    buf = Util.enlargeByteBuffer(buf,
                                                 MemoryPolicy.IGNORE,
                                                 Consts.MAX_BUF_SIZE);
                }
                buf.put(b);
            }
        } catch (OverflowException e) {
            throw new RsyncProtocolException(e);
        } catch (ChannelEOFException e) {
            // EOF is OK
        }
        buf.flip();
        return _characterDecoder.decode(buf);
    }

    private Collection<String> receiveArguments()
        throws ChannelException
    {
        Collection<String> list = new LinkedList<>();
        while (true) {
            String arg = readStringUntilNullOrEof();
            if (arg.isEmpty()) {
                break;
            }
            list.add(arg);
        }
        return list;
    }

    private void parseArguments(Collection<String> receivedArguments)
        throws ArgumentParsingError
    {
        ArgumentParser argsParser =
            ArgumentParser.newWithUnnamed("", "files...");
        // NOTE: has no argument handler
        argsParser.add(Option.newWithoutArgument(Option.Policy.REQUIRED,
                                                 "server", "", "", null));

        argsParser.add(Option.newWithoutArgument(
            Option.Policy.OPTIONAL,
            "sender", "", "",
            new Option.Handler() {
                @Override public void handle(Option option) {
                    setIsSender();
                }}));

        argsParser.add(Option.newWithoutArgument(
            Option.Policy.OPTIONAL,
            "recursive", "r", "",
            new Option.Handler() {
                @Override public void handle(Option option) {
                    enableRecursiveTransfer();
                }}));

        argsParser.add(Option.newStringOption(
            Option.Policy.REQUIRED,
            "rsh", "e", "",
            new Option.Handler() {
                @Override public void handle(Option option) {
                    parsePeerCompatibilites((String) option.getValue());
                }}));

        argsParser.add(Option.newWithoutArgument(
            Option.Policy.OPTIONAL,
            "verbose", "v", "",
            new Option.Handler() {
                @Override public void handle(Option option) {
                    increaseVerbosity();
                }}));

        argsParser.add(Option.newWithoutArgument(
            Option.Policy.OPTIONAL,
            "times", "t", "",
            new Option.Handler() {
                @Override public void handle(Option option) {
                    setIsPreserveTimes();
                }}));

        argsParser.parse(receivedArguments);
        assert !_isRecursiveTransfer || _isIncrementalRecurse :
               "We support only incremental recursive transfers for now";

        if (!isSender() && _module.isReadOnly()) {
            throw new RsyncProtocolException(
                String.format("Error: module %s is read only", _module));
        }

        List<String> unnamed = argsParser.getUnnamedArguments();
        if (unnamed.size() < 2) {
            throw new RsyncProtocolException(
                String.format("Got too few unnamed arguments from peer " +
                              "(%d), expected \".\" and more", unnamed.size()));
        }
        String dotSeparator = unnamed.remove(0);
        if (!dotSeparator.equals(Text.DOT)) {
            throw new RsyncProtocolException(
                String.format("Expected first non option-argument to be " +
                              "\".\", received \"%s\"", dotSeparator));
        }

        if (isSender()) {
            for (String fileName : unnamed) {
                Path safePath = _module.resolveVirtual(Paths.get(fileName));
                if (Text.isNameDotDir(fileName)) {
                    safePath = safePath.resolve(PathOps.DOT_DIR);
                }
                _sourceFiles.add(safePath);
            }
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("sender source files: " + _sourceFiles);
            }
        } else {
            if (unnamed.size() != 1) {
                throw new RsyncProtocolException(String.format(
                    "Error: expected exactly one file argument: %s contains %d",
                    unnamed, unnamed.size()));
            }
            String fileName = unnamed.get(0);
            Path safePath = _module.resolveVirtual(Paths.get(fileName));
            _receiverDestination = safePath.normalize();

            if (_log.isLoggable(Level.FINE)) {
                _log.fine("receiver destination: " + _receiverDestination);
            }
        }
    }

    private void increaseVerbosity()
    {
        _verbosity++;
    }

    private void enableRecursiveTransfer()
    {
        _isRecursiveTransfer = true;
    }

    // @throws RsyncProtocolException
    private void parsePeerCompatibilites(String str)
    {
        if (str.startsWith(Text.DOT)) {
            if (str.contains("i")) { // CF_INC_RECURSE
                assert _isRecursiveTransfer;
                _isIncrementalRecurse = true; // only set by client on --recursive or -r, but can also be disabled, we require it however (as a start)
            }
            if (str.contains("L")) { // CF_SYMLINK_TIMES
            }
            if (str.contains("s")) { // CF_SYMLINK_ICONV
            }
            if (str.contains("f")) { // CF_SAFE_FLIST
                // NOP, corresponds to use_safe_inc_flist in native rsync
            } else {
                throw new RsyncProtocolException(
                    String.format("Peer does not support safe file lists: %s",
                                  str));
            }
        } else {
            throw new RsyncProtocolException(
                String.format("Protocol not supported - got %s from peer",
                              str));
        }
    }

    private void sendCompatibilities() throws ChannelException
    {
        byte flags = RsyncCompatibilities.CF_SAFE_FLIST;
        if (_isIncrementalRecurse) {
            flags |= RsyncCompatibilities.CF_INC_RECURSE;
        }
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("> (we support) " + flags);
        }
        _peerConnection.putByte(flags);
    }

    private void sendChecksumSeed() throws ChannelException
    {
        assert _checksumSeed != null;
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("> (checksum seed) " +
                       BitOps.toBigEndianInt(_checksumSeed));
        }
        _peerConnection.putInt(BitOps.toBigEndianInt(_checksumSeed));
    }

    private void setIsPreserveTimes()
    {
        _isPreserveTimes = true;
    }

    public boolean isSender()
    {
        return _isSender;
    }

    public List<Path> sourceFiles()
    {
        return _sourceFiles;
    }

    public boolean isRecursive()
    {
        return _isRecursiveTransfer;
    }

    public boolean isPreserveTimes()
    {
        return _isPreserveTimes;
    }

    public Path getReceiverDestination()
    {
        assert _receiverDestination != null;
        return _receiverDestination;
    }

    private void setIsSender()
    {
        _isSender = true;
    }

    private String receiveModule() throws ChannelException
    {
        return readLine();
    }
}
