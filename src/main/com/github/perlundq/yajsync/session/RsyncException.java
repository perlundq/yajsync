package com.github.perlundq.yajsync.session;

@SuppressWarnings("serial")
public class RsyncException extends Exception
{
    public RsyncException(String msg)
    {
        super(msg);
    }

    public RsyncException(Throwable t)
    {
        super(t);
    }
}
