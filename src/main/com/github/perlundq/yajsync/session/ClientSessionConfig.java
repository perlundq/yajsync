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
package com.github.perlundq.yajsync.session;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.security.RsyncAuthContext;
import com.github.perlundq.yajsync.text.TextConversionException;
import com.github.perlundq.yajsync.util.BitOps;

public class ClientSessionConfig extends SessionConfig
{
    public interface AuthProvider {
        String getUser();
        char[] getPassword();
    }

    private static final Logger _log =
        Logger.getLogger(ClientSessionConfig.class.getName());
    private final boolean _isRecursive;

    /**
     * @throws IllegalArgumentException if charset is not supported
     */
    private ClientSessionConfig(ReadableByteChannel in, WritableByteChannel out,
                                Charset charset, boolean isRecursive)
    {
        super(in, out, charset);
        _isRecursive = isRecursive;
    }

    /**
     * @throws IllegalArgumentException if charset is not supported
     * @throws RsyncProtocolException if we or peer fails to adhere to the rsync
     *         handshake protocol
     * @throws RsyncProtocolException if failing to encode/decode characters
     *         correctly
     */
    public static ClientSessionConfig handshake(ReadableByteChannel in,
                                                WritableByteChannel out,
                                                Charset charset,
                                                boolean isRecursive,
                                                String moduleName,
                                                Iterable<String> args,
                                                AuthProvider authProvider)
        throws ChannelException
    {
        try {
            ClientSessionConfig instance = new ClientSessionConfig(in,
                                                                   out,
                                                                   charset,
                                                                   isRecursive);

            instance.exchangeProtocolVersion();
            instance.sendModule(moduleName);
            instance.printLinesAndGetReplyStatus(authProvider);
            if (instance.status() != SessionStatus.OK) {
                return instance;
            }

            assert !moduleName.isEmpty();
            instance.sendArguments(args);
            instance.receiveCompatibilities();
            instance.receiveChecksumSeed();
            return instance;
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        }
    }

    /**
     * @throws TextConversionException
     */
    private void sendModule(String moduleName) throws ChannelException
    {
        writeString(moduleName + '\n');
    }

    private void printLinesAndGetReplyStatus(AuthProvider authProvider)
        throws ChannelException
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
                System.err.println(line);
                _status = SessionStatus.ERROR;
                return;
            } else if (line.startsWith(SessionStatus.AUTHREQ.toString())) {
                String challenge =
                    line.substring(SessionStatus.AUTHREQ.toString().length());
                sendAuthResponse(authProvider, challenge);
            } else {
                System.out.println(line); // FIXME: don't hardcode System.out
            }
        }
    }

    /**
     * @throws TextConversionException
     */
    private void sendAuthResponse(AuthProvider authProvider, String challenge)
        throws ChannelException
    {
        String user = authProvider.getUser();
        char[] password = authProvider.getPassword();
        try {
            RsyncAuthContext authContext =
                RsyncAuthContext.fromChallenge(_characterEncoder, challenge);
            String response = authContext.response(password);
            writeString(String.format("%s %s\n", user, response));
        } finally {
            Arrays.fill(password, (char) 0);
        }
    }

    /**
     * @throws TextConversionException
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

    private void receiveCompatibilities() throws ChannelException
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
        if ((flags & RsyncCompatibilities.CF_SAFE_FLIST) == 0) {
            throw new RsyncProtocolException("peer does not support " +
                                             "safe file lists");
        }
    }

    private void receiveChecksumSeed() throws ChannelException
    {
        int seedValue = _peerConnection.getInt();
        _checksumSeed = BitOps.toLittleEndianBuf(seedValue);
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("< (checksum seed) " + seedValue);
        }
    }
}
