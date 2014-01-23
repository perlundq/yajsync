package com.github.perlundq.yajsync.test;

import java.nio.ByteBuffer;

import com.github.perlundq.yajsync.channels.ChannelException;
import com.github.perlundq.yajsync.channels.Readable;
import com.github.perlundq.yajsync.util.Util;

public class ReadableByteBuffer implements Readable
{
    private final ByteBuffer _buf;

    public ReadableByteBuffer(ByteBuffer buf) {
        _buf = buf;
    }

    @Override
    public void get(byte[] dst, int offset, int length) throws ChannelException
    {
        _buf.get(dst, offset, length);
    }

    @Override
    public ByteBuffer get(int numBytes) throws ChannelException
    {
        return Util.slice(_buf, 0, numBytes);
    }

    @Override
    public byte getByte() throws ChannelException
    {
        return _buf.get();
    }

    @Override
    public char getChar() throws ChannelException
    {
        return _buf.getChar();
    }

    @Override
    public int getInt() throws ChannelException
    {
        return _buf.getInt();
    }

    @Override
    public void skip(int numBytes) throws ChannelException
    {
        _buf.position(_buf.position() + numBytes);
    }
}