package com.github.perlundq.yajsync.filelist;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import com.github.perlundq.yajsync.attr.FileInfo;
import com.github.perlundq.yajsync.attr.Group;
import com.github.perlundq.yajsync.attr.RsyncFileAttributes;
import com.github.perlundq.yajsync.attr.User;
import com.github.perlundq.yajsync.internal.util.FileOps;

public class FileInfoTest {

    RsyncFileAttributes _fileAttrs =
        new RsyncFileAttributes(FileOps.S_IFREG | 0644,
                                0, 0, User.JVM_USER, Group.JVM_GROUP);
    RsyncFileAttributes _dirAttrs =
        new RsyncFileAttributes(FileOps.S_IFDIR | 0755,
                                0, 0, User.JVM_USER, Group.JVM_GROUP);
    Path _dotDirPath = Paths.get("./");
    Path dotDirAbsPath = Paths.get("/path/to/module/root/");
    FileInfo _dotDir = new FileInfo(dotDirAbsPath,
                                    _dotDirPath,
                                    _dotDirPath.toString().getBytes(),
                                    _dirAttrs);

    @Test
    public void testSortDotDirWithDotDirEqual()
    {
        assertTrue(_dotDir.equals(_dotDir));
        assertTrue(_dotDir.compareTo(_dotDir) == 0);
    }

    @Test
    public void testSortDotDirBeforeNonDotDir()
    {
        Path p = Paths.get(".a");
        FileInfo f = new FileInfo(_dotDirPath.resolve(p), p,
                                  p.toString().getBytes(), _fileAttrs);
        assertFalse(_dotDir.equals(f));
        assertTrue(_dotDir.compareTo(f) == -1);
    }

    @Test
    public void testSortDotDirBeforeNonDotDir2()
    {
        Path p = Paths.get("...");
        FileInfo f = new FileInfo(_dotDirPath.resolve(p), p,
                                  p.toString().getBytes(), _fileAttrs);
        assertFalse(_dotDir.equals(f));
        assertTrue(_dotDir.compareTo(f) == -1);
    }

    @Test
    public void testSortDotDirBeforeNonDotDir3()
    {
        Path p = Paths.get("...");
        FileInfo f = new FileInfo(_dotDirPath.resolve(p), p,
                                  p.toString().getBytes(), _dirAttrs);
        assertFalse(_dotDir.equals(f));
        assertTrue(_dotDir.compareTo(f) == -1);
    }

    // test empty throws illegalargumentexception
    // test dot is always a directory

    /*
     * ./
     * ...
     * ..../
     * .a
     * a.
     * a..
     * ..a
     * .a.
     *
     * a.
     * a../
     * .a/
     * .a/b
     * b/
     * b/.
     * c
     * cc
     * c.c
     */

    @Test
    public void testSortDotDirBeforeNonDotDir4()
    {
        Path p = Paths.get("a.");
        FileInfo f = new FileInfo(_dotDirPath.resolve(p), p,
                                  "a./".getBytes(), _dirAttrs);
        assertFalse(_dotDir.equals(f));
        assertTrue(_dotDir.compareTo(f) == -1);
    }
}