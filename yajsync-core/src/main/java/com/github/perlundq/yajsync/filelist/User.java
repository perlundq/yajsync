package com.github.perlundq.yajsync.filelist;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Objects;

import com.github.perlundq.yajsync.util.Environment;

public final class User
{
    public static final int UID_MAX = 65535;

    private static final int UID_NOBODY = UID_MAX - 1;
    private static final int MAX_NAME_LENGTH = 255;
    private static final User ROOT = new User("root", 0);
    private static final User NOBODY = new User("nobody", UID_NOBODY);
    private static final User JVM_USER = new User(Environment.getUserName(),
                                                  Environment.getUserId());

    private final String _name;
    private final int _uid;

    public User(String name, int uid)
    {
        if (name == null) {
            throw new IllegalArgumentException();
        }
        if (name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException();
        }
        if (uid < 0 || uid > UID_MAX) {
            throw new IllegalArgumentException();
        }
        _name = name;
        _uid = uid;
    }

    public static User whoami()
    {
        return JVM_USER;
    }

    public static User root()
    {
        return ROOT;
    }

    public static User nobody()
    {
        return NOBODY;
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s, %d)",
                             getClass().getSimpleName(), _name, _uid);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        } else if (other != null && getClass() == other.getClass()) {
            User otherUser = (User) other;
            return _uid == otherUser._uid && _name.equals(otherUser._name);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_name, _uid);
    }

    public String name()
    {
        return _name;
    }

    public int uid()
    {
        return _uid;
    }

    public UserPrincipal userPrincipal() throws IOException
    {
        UserPrincipalLookupService lookupService =
            FileSystems.getDefault().getUserPrincipalLookupService();
        return lookupService.lookupPrincipalByName(_name);
    }
}
