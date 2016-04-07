package com.github.perlundq.yajsync.internal.util;

public class StatusResult<T>
{
    private final boolean _isOK;
    private final T _value;

    public StatusResult(boolean isOK, T value)
    {
        _isOK = isOK;
        _value = value;
    }

    public boolean isOK()
    {
        return _isOK;
    }

    public T value()
    {
        return _value;
    }
}