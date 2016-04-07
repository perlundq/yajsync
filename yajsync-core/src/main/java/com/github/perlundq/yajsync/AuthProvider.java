package com.github.perlundq.yajsync;

import java.io.IOException;

public interface AuthProvider
{
    String getUser() throws IOException;
    char[] getPassword() throws IOException;
}
