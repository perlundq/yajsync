/*
 * @(#) DirectByteBufferUtils.java
 * Created Jun 15, 2020 by oleg
 * (C) Odnoklassniki.ru
 */
package com.github.perlundq.yajsync.internal.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.internal.session.Receiver;

/**
 * A safe wrapper round java cleaner
 */
public class DirectByteBufferCleaner
{
    private static final Logger _log =
                    Logger.getLogger(Receiver.class.getName());
    private static final Method cleanerMethod, cleanMethod;

    public static final boolean isCleanerAvailable;

    static
    {
        Method m, m1;
        try
        {
            ByteBuffer buf = ByteBuffer.allocateDirect(1);
            m = buf.getClass().getMethod("cleaner");
            m.setAccessible( true );
            Object cleaner = m.invoke(buf);
            m1 = cleaner.getClass().getMethod("clean");            
        }
        catch (Exception e)
        {
            // Perhaps a non-sun-derived JVM - contributions welcome
            e.printStackTrace();
            _log.info("Cannot initialize direct bute buffer cleaner");
            m = null;
            m1 = null;
        }
        cleanerMethod = m;
        cleanMethod = m1;
        isCleanerAvailable = m != null && m1 != null;
    }

    public static void clean(ByteBuffer buffer)
    {
        if (!isCleanerAvailable)
            return;
        
        try
        {
            cleanMethod.invoke( cleanerMethod.invoke( buffer ) );
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }

}
