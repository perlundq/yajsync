package com.github.perlundq.yajsync.util;

import static org.junit.Assert.assertTrue;

import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.util.PathOps;

public class PathOpsTest
{
    @Test(expected=InvalidPathException.class)
    public void testNormalizeIllegal()
    {
        PathOps.normalizeStrict(Paths.get(".."));
    }

    @Test
    public void testPreserveTrailingSlash()
    {
        String pathName = "a/b/";
        Path path = PathOps.get(FileSystems.getDefault(), pathName);
        assertTrue(path.endsWith(Text.DOT));
    }

    @Test
    public void testEmptyToDotDir()
    {
        String pathName = "";
        Path path = PathOps.get(FileSystems.getDefault(), pathName);
        assertTrue(path.equals(Paths.get(Text.DOT)));
    }

    @Test
    public void testPreserveDot()
    {
        String pathName = ".";
        Path path = PathOps.get(FileSystems.getDefault(), pathName);
        assertTrue(path.equals(Paths.get(Text.DOT)));
    }

    @Test
    public void testSubtractPaths1()
    {
        Path expected = Paths.get("/");
        Path sub = Paths.get(Text.DOT);
        Path parent = expected.resolve(sub);
        Path res = PathOps.subtractPathOrNull(parent, sub);
        assertTrue(res.equals(expected));
    }

    @Test
    public void testSubtractPaths2()
    {
        Path expected = Paths.get("/a");
        Path sub = Paths.get("b");
        Path parent = expected.resolve(sub);
        Path res = PathOps.subtractPathOrNull(parent, sub);
        assertTrue(res.equals(expected));
    }

    @Test
    public void testSubtractPaths3()
    {
        Path expected = Paths.get("a");
        Path sub = Paths.get("b");
        Path parent = expected.resolve(sub);
        Path res = PathOps.subtractPathOrNull(parent, sub);
        assertTrue(res.equals(expected));
    }

    @Test
    public void testSubtractPaths4()
    {
        Path expected = Paths.get("/");
        Path sub = Paths.get("/");
        Path parent = expected.resolve(sub);
        Path res = PathOps.subtractPathOrNull(parent, sub);
        assertTrue(res.equals(expected));
    }

    @Test
    public void testSubtractPaths5()
    {
        Path expected = Paths.get("/");
        Path sub = Paths.get("/a");
        Path parent = expected.resolve(sub);
        Path res = PathOps.subtractPathOrNull(parent, sub);
        assertTrue(res.equals(expected));
    }

    @Test
    public void testSubtractPaths6()
    {
        Path sub = Paths.get("a");
        Path parent = Paths.get("a");
        Path res = PathOps.subtractPathOrNull(parent, sub);
        assertTrue(res == null);
    }

    @Test
    public void testSubtractPaths7()
    {
        Path expected = Paths.get("././.");
        Path sub = Paths.get(".");
        Path parent = expected.resolve(sub);
        Path res = PathOps.subtractPathOrNull(parent, sub);
        assertTrue(res.equals(expected));
    }
}
