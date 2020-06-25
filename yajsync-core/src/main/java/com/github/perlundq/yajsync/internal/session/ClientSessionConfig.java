/*
 * Rsync client -> server handshaking protocol
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
package com.github.perlundq.yajsync.internal.session;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.AuthProvider;
import com.github.perlundq.yajsync.RsyncException;
import com.github.perlundq.yajsync.RsyncProtocolException;
import com.github.perlundq.yajsync.internal.channels.ChannelException;
import com.github.perlundq.yajsync.internal.text.TextConversionException;
import com.github.perlundq.yajsync.internal.util.BitOps;
import com.github.perlundq.yajsync.internal.util.Pair;
import com.github.perlundq.yajsync.server.module.RsyncAuthContext;

public class ClientSessionConfig extends SessionConfig
{
    private static final Logger _log =
        Logger.getLogger(ClientSessionConfig.class.getName());
    private final boolean _isRecursive;
    private final PrintStream _err;
    private final BlockingQueue<Pair<Boolean, String>> _listing =
            new LinkedBlockingQueue<>();
    private boolean _isSafeFileList;


    /**
     * @param checksumHash 
     * @throws IllegalArgumentException if charset is not supported
     */
    public ClientSessionConfig(ReadableByteChannel in, WritableByteChannel out,
                               Charset charset, ChecksumHash checksumHash, boolean isRecursive,
                               PrintStream stderr)
    {
        super(in, out, charset);
        _isRecursive = isRecursive;
        _err = stderr;
        _checksumHash = checksumHash;
    }

    /**
     * @throws RsyncProtocolException if peer fails to adhere to the rsync
     *         handshake protocol
     * @throws ChannelException if there is a communication failure with peer
     * @throws IllegalStateException if failing to encode output characters
     *         using current character set
     * @throws IllegalArgumentException if charset is not supported
     */
    public SessionStatus handshake(String moduleName,
                                   Iterable<String> args,
                                   AuthProvider authProvider)
        throws RsyncException
    {
        try {
            exchangeProtocolVersion();
            sendModule(moduleName);
            printLinesAndGetReplyStatus(authProvider);
            if (_status != SessionStatus.OK) {
                return _status;
            }

            assert !moduleName.isEmpty();
            sendArguments(args);
            receiveCompatibilities();
            receiveChecksumSeed();
            return _status;
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        } finally {
            Pair<Boolean, String> poisonPill = new Pair<>(false, null);
            _listing.add(poisonPill);
        }
    }

    public BlockingQueue<Pair<Boolean, String>> modules()
    {
        return _listing;
    }

    public boolean isSafeFileList()
    {
        return _isSafeFileList;
    }


    /**
     * @throws ChannelException if there is a communication failure with peer
     * @throws IllegalStateException if failing to encode output characters
     *         using current character set
     */
    private void sendModule(String moduleName) throws ChannelException
    {
        writeString(moduleName + '\n');
    }

    /**
     * @throws RsyncException if failing to provide a username and/or password
     * @throws RsyncProtocolException if peer sent premature null character
     * @throws RsyncProtocolException if peer sent too large amount of
     *         characters
     * @throws ChannelException if there is a communication failure with peer
     */
    private void printLinesAndGetReplyStatus(AuthProvider authProvider)
        throws RsyncException
    {
        while (true) {
            String line = readLine();
            if (line.equals(SessionStatus.OK.toString())) {
                _status = SessionStatus.OK;
                return;
            } else if (line.equals(SessionStatus.EXIT.toString())) {
                _status = SessionStatus.EXIT;
                return;
            } else if (line.startsWith(SessionStatus.ERROR.toString())) {
                _err.println(line);
                _status = SessionStatus.ERROR;
                return;
            } else if (line.startsWith(SessionStatus.AUTHREQ.toString())) {
                String challenge =
                    line.substring(SessionStatus.AUTHREQ.toString().length());
                sendAuthResponse(authProvider, challenge);
            } else {
                _listing.add(new Pair<>(true, line));
            }
        }
    }

    /**
     * @throws RsyncException if failing to provide a username and/or password
     * @throws ChannelException if there is a communication failure with peer
     */
    private void sendAuthResponse(AuthProvider authProvider, String challenge)
        throws RsyncException
    {
        try {
            String user = authProvider.getUser();
            char[] password = authProvider.getPassword();
            try {
                RsyncAuthContext authContext =
                        RsyncAuthContext.fromChallenge(_characterEncoder,
                                                       challenge);
                String response = authContext.response(password);
                writeString(String.format("%s %s\n", user, response));
            } finally {
                Arrays.fill(password, (char) 0);
            }
        } catch (IOException | TextConversionException e) {
            throw new RsyncException(e);
        }
    }

    /**
     * @throws IllegalStateException if failing to encode output characters
     *         using current character set
     */
    private void sendArguments(Iterable<String> serverArgs)
        throws ChannelException
    {
        for (String arg : serverArgs) {
            writeString(arg);
            _peerConnection.putByte((byte) 0);
        }
        _peerConnection.putByte((byte) 0);
    }

    /**
     * @throws ChannelException if there is a communication failure with peer
     * @throws RsyncProtocolException if peer protocol is incompatible with ours
     */
    private void receiveCompatibilities() throws ChannelException,
                                                 RsyncProtocolException
    {
        byte flags = _peerConnection.getByte();
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("< (peer supports) " + flags);
        }
        if (_isRecursive &&
            (flags & RsyncCompatibilities.CF_INC_RECURSE) == 0) {
            throw new RsyncProtocolException("peer does not support " +
                                             "incremental recurse");
        }
        _isSafeFileList = (flags & RsyncCompatibilities.CF_SAFE_FLIST) != 0;
    }

    private void receiveChecksumSeed() throws ChannelException
    {
        int seedValue = _peerConnection.getInt();
        _checksumSeed = seedValue;
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("< (checksum seed) " + seedValue);
        }
    }
}
