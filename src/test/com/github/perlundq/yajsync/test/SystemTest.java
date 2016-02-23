package com.github.perlundq.yajsync.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.github.perlundq.yajsync.filelist.RsyncFileAttributes;
import com.github.perlundq.yajsync.filelist.User;
import com.github.perlundq.yajsync.security.RsyncAuthContext;
import com.github.perlundq.yajsync.session.Module;
import com.github.perlundq.yajsync.session.ModuleException;
import com.github.perlundq.yajsync.session.ModuleProvider;
import com.github.perlundq.yajsync.session.Modules;
import com.github.perlundq.yajsync.session.RestrictedModule;
import com.github.perlundq.yajsync.session.RestrictedPath;
import com.github.perlundq.yajsync.session.Statistics;
import com.github.perlundq.yajsync.ui.SyncClient;
import com.github.perlundq.yajsync.ui.YajSyncServer;
import com.github.perlundq.yajsync.util.FileOps;
import com.github.perlundq.yajsync.util.Option;
import com.github.perlundq.yajsync.util.PathOps;



class FileUtil
{
    public static byte[] generateBytes(int content, int num)
    {
        byte[] res = new byte[num];
        for (int i = 0; i < num; i++) {
            res[i] = (byte) content;
        }
        return res;
    }

    public static void writeToFiles(byte[] content, Path ...path)
        throws IOException
    {
        for (Path p : path) {
            try (FileOutputStream out = new FileOutputStream(p.toFile())) {
                out.write(content);
            }
        }
    }

    public static void writeToFiles(int content, Path ...path)
        throws IOException
    {
        for (Path p : path) {
            try (FileOutputStream out = new FileOutputStream(p.toFile())) {
                out.write(content);
            }
        }
    }

    public static boolean isContentIdentical(Path leftPath, Path rightPath)
        throws IOException
    {
        try (InputStream left_is = Files.newInputStream(leftPath);
             InputStream right_is = Files.newInputStream(rightPath)) {
            while (true) {
                int left_byte = left_is.read();
                int right_byte = right_is.read();
                if (left_byte != right_byte) {
                    return false;
                }
                boolean isEOF = left_byte == -1; // && right_byte == -1;
                if (isEOF) {
                    return true;
                }
            }
        }
    }

    private static boolean isFileSameTypeAndSize(RsyncFileAttributes leftAttrs,
                                                 RsyncFileAttributes rightAttrs)
    {
        int leftType = FileOps.fileType(leftAttrs.mode());
        int rightType = FileOps.fileType(rightAttrs.mode());
        return leftType == rightType && (!FileOps.isRegularFile(leftType) ||
                                         leftAttrs.size() == rightAttrs.size());
    }

    private static SortedMap<Path, Path> listDir(Path path) throws IOException
    {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            SortedMap<Path, Path> files = new TreeMap<>();
            for (Path p : stream) {
                files.put(p.getFileName(), p);
            }
            return files;
        }
    }

    public static boolean isDirectoriesIdentical(Path leftDir, Path rightDir)
        throws IOException
    {
        SortedMap<Path, Path> leftFiles = FileUtil.listDir(leftDir);
        SortedMap<Path, Path> rightFiles = FileUtil.listDir(rightDir);

        if (!leftFiles.keySet().equals(rightFiles.keySet())) {
            return false;
        }

        for (Map.Entry<Path, Path> entrySet : leftFiles.entrySet()) {
            Path name = entrySet.getKey();
            Path leftPath = entrySet.getValue();
            Path rightPath = rightFiles.get(name);

            RsyncFileAttributes leftAttrs = RsyncFileAttributes.stat(leftPath);
            RsyncFileAttributes rightAttrs =
                RsyncFileAttributes.stat(rightPath);
            if (!FileUtil.isFileSameTypeAndSize(leftAttrs, rightAttrs)) {
                return false;
            } else if (leftAttrs.isRegularFile()) {
                boolean isIdentical = FileUtil.isContentIdentical(leftPath,
                                                                  rightPath);
                if (!isIdentical) {
                    return false;
                }
            } else if (leftAttrs.isDirectory()) {
                boolean isIdentical =
                    FileUtil.isDirectoriesIdentical(leftPath, rightPath);
                if (!isIdentical) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isFileSameOwner(RsyncFileAttributes leftAttrs,
                                          RsyncFileAttributes rightAttrs)
    {
        return leftAttrs.user().equals(rightAttrs.user());
    }

    public static boolean isFileSameGroup(RsyncFileAttributes leftAttrs,
                                          RsyncFileAttributes rightAttrs)
    {
        return leftAttrs.group().equals(rightAttrs.group());
    }

    public static boolean isDirectory(Path path)
    {
        return Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS);
    }

    public static boolean isFile(Path path)
    {
        return Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS);
    }

    public static boolean exists(Path path)
    {
        return Files.exists(path, LinkOption.NOFOLLOW_LINKS);
    }

    public static long du(Path... srcFiles) throws IOException
    {
        long size = 0;
        for (Path p : srcFiles) {
            size += Files.size(p);
        }
        return size;
    }

    public static File createPasswordFile(String authToken) throws IOException {

        File passwordFile = File.createTempFile("password-file", ".tmp");
        try (PrintStream out = new PrintStream(passwordFile)) {
            out.print(authToken);
        }

        Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
        // add owners permission
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        // add group permissions
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.GROUP_WRITE);
        perms.add(PosixFilePermission.GROUP_EXECUTE);

        Files.setPosixFilePermissions(Paths.get(passwordFile.toURI()), perms);

        passwordFile.deleteOnExit();

        return passwordFile;
    }
}

class SimpleRestrictedModule extends RestrictedModule
{
    private final String _authToken;
    private final Module _module;
    private final String _name;
    private final String _comment;

    public SimpleRestrictedModule(String authToken, Module module, String name,
                                  String comment)
    {
        _authToken = authToken;
        _module = module;
        _name = name;
        _comment = comment;
    }

    @Override
    public String authenticate(RsyncAuthContext authContext, String userName)
    {
        return authContext.response(_authToken.toCharArray());
    }

    @Override
    public Module toModule()
    {
        return _module;
    }

    @Override
    public String name()
    {
        return _name;
    }

    @Override
    public String comment()
    {
        return _comment;
    }
}

class SimpleModule implements Module
{
    private final String _name;
    private final RestrictedPath _path;
    private final String _comment;
    private final boolean _isReadable;
    private final boolean _isWritable;

    SimpleModule(Path name, Path root, String comment,
                 boolean isReadable, boolean isWritable)
    {
        _name = name.toString();
        _path = new RestrictedPath(name, root);
        _comment = comment;
        _isReadable = isReadable;
        _isWritable = isWritable;
    }

    @Override
    public String name()
    {
        return _name;
    }

    @Override
    public String comment()
    {
        return _comment;
    }

    @Override
    public RestrictedPath restrictedPath()
    {
        return _path;
    }

    @Override
    public boolean isReadable()
    {
        return _isReadable;
    }

    @Override
    public boolean isWritable()
    {
        return _isWritable;
    }
}

class TestModules implements Modules
{
    private final Map<String, Module> _modules;

    TestModules(Module... modules)
    {
        _modules = new HashMap<>();
        for (Module module : modules) {
            _modules.put(module.name(), module);
        }
    }

    @Override
    public Module get(String moduleName) throws ModuleException
    {
        Module module = _modules.get(moduleName);
        if (module == null) {
            throw new ModuleException("no such module: " + moduleName);
        }
        return module;
    }

    @Override
    public Iterable<Module> all()
    {
        return _modules.values();
    }
}

class TestModuleProvider extends ModuleProvider
{
    private final Modules _modules;

    TestModuleProvider(Modules modules)
    {
        _modules = modules;
    }

    @Override
    public Collection<Option> options()
    {
        return Collections.emptyList();
    }

    @Override
    public void close()
    {
        /* nop */
    }

    @Override
    public Modules newAuthenticated(InetAddress address, Principal principal)
    {
        return _modules;
    }

    @Override
    public Modules newAnonymous(InetAddress address)
    {
        return _modules;
    }
}

public abstract class SystemTest
{
    protected static class ReturnStatus
    {
        final int rc;
        final Statistics stats;

        ReturnStatus(int rc_, Statistics stats_)
        {
            rc = rc_;
            stats = stats_;
        }
    }

    protected final PrintStream _nullOut =
        new PrintStream(new OutputStream() {
            @Override
            public void write(int b) { /* nop */}
        }
    );

    protected ExecutorService _service;

    protected abstract SyncClient newClient();

    protected YajSyncServer newServer(Modules modules)
    {
        YajSyncServer server = new YajSyncServer().setStandardOut(_nullOut).
                                                   setStandardErr(_nullOut);
        server.setModuleProvider(new TestModuleProvider(modules));
        return server;
    }

    protected abstract ReturnStatus fileCopy(boolean startServer, Path src,
            Path dst, String... args);

    protected abstract ReturnStatus fileCopy(Path src, Path dst,
            String... args);

    private ReturnStatus recursiveCopyTrailingSlash(Path src, Path dst)
    {
        SyncClient client = newClient();
        int rc = client.start(new String[] { "--recursive",
                                             src.toString() + "/",
                                             dst.toString() });
        return new ReturnStatus(rc, client.statistics());
    }

    @Before
    public void setup()
    {
        _service = Executors.newCachedThreadPool();
    }

    @After
    public void teardown()
    {
        _service.shutdownNow();
    }

    @Rule
    public final TemporaryFolder _tempDir = new TemporaryFolder();

    @Test
    public void testFileUtilIdenticalEmptyDirs() throws IOException
    {
        Path left = _tempDir.newFolder("left_dir").toPath();
        Path right = _tempDir.newFolder("right_dir").toPath();
        assertTrue(FileUtil.isDirectoriesIdentical(left, right));
    }

    @Test
    public void testFileUtilNotIdenticalDirs() throws IOException
    {
        Path left = _tempDir.newFolder("left_dir").toPath();
        Path right = _tempDir.newFolder("right_dir").toPath();
        Files.createFile(left.resolve("file1"));
        assertFalse(FileUtil.isDirectoriesIdentical(left, right));
    }

    @Test
    public void testFileUtilIdenticalEmptyFiles() throws IOException
    {
        Path left = _tempDir.newFile("left_file").toPath();
        Path right = _tempDir.newFile("right_file").toPath();
        assertTrue(FileUtil.isContentIdentical(left, right));
    }

    @Test
    public void testFileUtilIdenticalFiles() throws IOException
    {
        Path left = _tempDir.newFile("left_file").toPath();
        Path right = _tempDir.newFile("right_file").toPath();
        FileUtil.writeToFiles(127, left, right);
        assertTrue(FileUtil.isContentIdentical(left, right));
    }

    @Test
    public void testFileUtilNotIdenticalFiles() throws IOException
    {
        Path left = _tempDir.newFile("left_file").toPath();
        Path right = _tempDir.newFile("right_file").toPath();
        FileUtil.writeToFiles(127, left);
        FileUtil.writeToFiles(128, right);
        assertFalse(FileUtil.isContentIdentical(left, right));
    }

    @Test
    public void testFileUtilIdenticalDirsWithSymlinks() throws IOException
    {
        Path left = _tempDir.newFolder("left_dir").toPath();
        Path right = _tempDir.newFolder("right_dir").toPath();
        Path left_file = Files.createFile(left.resolve("file1"));
        Path right_file = Files.createFile(right.resolve("file1"));
        Files.createSymbolicLink(left.resolve("link1"), left_file);
        Files.createSymbolicLink(right.resolve("link1"), right_file);
        assertTrue(FileUtil.isDirectoriesIdentical(left, right));
    }

    @Test
    public void testFileUtilNotIdenticalDirs2() throws IOException
    {
        Path left = _tempDir.newFolder("left_dir").toPath();
        Path right = _tempDir.newFolder("right_dir").toPath();
        Path left_file = Files.createFile(left.resolve("file1"));
        Files.createFile(right.resolve("file1"));
        FileUtil.writeToFiles(0, left_file);
        assertFalse(FileUtil.isDirectoriesIdentical(left, right));
    }

    @Test
    public void testClientNoArgs()
    {
        int rc = newClient().start(new String[] {});
        assertTrue(rc == -1 || rc == 1);
    }

    @Test
    public void testClientHelp()
    {
        int rc = newClient().start(new String[] { "--help" });
        assertTrue(rc == 0);
    }

    @Test
    public void testClientSingleFileCopy() throws IOException
    {
        Path src = _tempDir.newFile().toPath();
        Path dst = _tempDir.newFile().toPath();
        FileUtil.writeToFiles(0, src);
        int numFiles = 1;
        long fileSize = Files.size(src);
        ReturnStatus status = fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.numFiles() == numFiles);
        assertTrue(status.stats.numTransferredFiles() == numFiles);
        assertTrue(status.stats.totalLiteralSize() == fileSize);
        assertTrue(status.stats.totalMatchedSize() == 0);
    }

    @Test
    public void testClientDotDotSrcArg() throws IOException
    {
        Path src = _tempDir.newFile().toPath();
        Path srcDotDot = _tempDir.newFolder().toPath().
                resolve(PathOps.DOT_DOT_DIR).
                resolve(src.getFileName());
        Path dst = _tempDir.newFile().toPath();
        FileUtil.writeToFiles(0, src);
        int numFiles = 1;
        long fileSize = Files.size(src);
        ReturnStatus status = fileCopy(srcDotDot, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.numFiles() == numFiles);
        assertTrue(status.stats.numTransferredFiles() == numFiles);
        assertTrue(status.stats.totalLiteralSize() == fileSize);
        assertTrue(status.stats.totalMatchedSize() == 0);
    }

    @Test
    public void testClientEmptyDirCopy() throws IOException
    {
        Path src = _tempDir.newFolder().toPath();
        Path dst = Paths.get(src.toString() + ".dst");
        Path copyOfSrc = dst.resolve(src.getFileName());
        int numDirs = 1;
        int numFiles = 0;
        long fileSize = 0;
        ReturnStatus status = fileCopy(src, dst, "--recursive");
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isDirectory(dst));
        assertTrue(FileUtil.isDirectoriesIdentical(src, copyOfSrc));
        assertTrue(status.stats.numFiles() == numDirs + numFiles);
        assertTrue(status.stats.numTransferredFiles() == numFiles);
        assertTrue(status.stats.totalLiteralSize() == fileSize);
        assertTrue(status.stats.totalMatchedSize() == 0);
    }

    @Test
    public void testClientEmptyDirTrailingSlashCopy() throws IOException
    {
        Path src = _tempDir.newFolder().toPath();
        Path dst = Paths.get(src.toString() + ".dst");
        int numDirs = 1;
        int numFiles = 0;
        long fileSize = 0;
        ReturnStatus status = recursiveCopyTrailingSlash(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isDirectory(dst));
        assertTrue(FileUtil.isDirectoriesIdentical(src, dst));
        assertTrue(status.stats.numFiles() == numDirs + numFiles);
        assertTrue(status.stats.numTransferredFiles() == numFiles);
        assertTrue(status.stats.totalLiteralSize() == fileSize);
        assertTrue(status.stats.totalMatchedSize() == 0);
    }

    @Test
    public void testSiblingSubdirsSubstring() throws IOException
    {
        Path src = _tempDir.newFolder().toPath();
        Path dst = _tempDir.newFolder().toPath();
        Path srcDir1 = src.resolve("dir");
        Path srcDir2 = src.resolve("dir.sub");
        Path srcFile1 = srcDir1.resolve("file1");
        Path srcFile2 = srcDir2.resolve("file2");
        Files.createDirectory(srcDir1);
        Files.createDirectory(srcDir2);
        FileUtil.writeToFiles(7, srcFile1);
        FileUtil.writeToFiles(8, srcFile2);
        int numDirs = 3;
        int numFiles = 2;
        long fileSize = FileUtil.du(srcFile1, srcFile2);
        ReturnStatus status = recursiveCopyTrailingSlash(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isDirectoriesIdentical(src, dst));
        assertTrue(status.stats.numFiles() == numDirs + numFiles);
        assertTrue(status.stats.numTransferredFiles() == numFiles);
        assertTrue(status.stats.totalLiteralSize() == fileSize);
        assertTrue(status.stats.totalMatchedSize() == 0);
    }

    @Test
    public void testCopyFileMultipleBlockSize() throws IOException
    {
        Path src = _tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 2048;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0xF0, fileSize);
        FileUtil.writeToFiles(content, src);
        ReturnStatus status = fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.numFiles() == numDirs + numFiles);
        assertTrue(status.stats.numTransferredFiles() == numFiles);
        assertTrue(status.stats.totalLiteralSize() == fileSize);
        assertTrue(status.stats.totalMatchedSize() == 0);
    }

    @Test
    public void testCopyFileSameBlockSize() throws IOException
    {
        Path src = _tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 512;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0xcd, fileSize);
        FileUtil.writeToFiles(content, src);
        ReturnStatus status = fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.numFiles() == numDirs + numFiles);
        assertTrue(status.stats.numTransferredFiles() == numFiles);
        assertTrue(status.stats.totalLiteralSize() == fileSize);
        assertTrue(status.stats.totalMatchedSize() == 0);
    }

    @Test
    public void testCopyFileLessThanBlockSize() throws IOException
    {
        Path src = _tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 257;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0xbc, fileSize);
        FileUtil.writeToFiles(content, src);
        ReturnStatus status = fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.numFiles() == numDirs + numFiles);
        assertTrue(status.stats.numTransferredFiles() == numFiles);
        assertTrue(status.stats.totalLiteralSize() == fileSize);
        assertTrue(status.stats.totalMatchedSize() == 0);
    }

    @Test
    public void testCopyFileNotMultipleBlockSize() throws IOException
    {
        Path src = _tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 651;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0x19, fileSize);
        FileUtil.writeToFiles(content, src);
        ReturnStatus status = fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.numFiles() == numDirs + numFiles);
        assertTrue(status.stats.numTransferredFiles() == numFiles);
        assertTrue(status.stats.totalLiteralSize() == fileSize);
        assertTrue(status.stats.totalMatchedSize() == 0);
    }

    @Test
    public void testCopyFileTwiceNotMultipleBlockSize() throws IOException
    {
        Path src = _tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 557;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0x18, fileSize);
        FileUtil.writeToFiles(content, src);
        Files.setLastModifiedTime(src, FileTime.fromMillis(0));
        ReturnStatus status = fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.numFiles() == numDirs + numFiles);
        assertTrue(status.stats.numTransferredFiles() == numFiles);
        assertTrue(status.stats.totalLiteralSize() == fileSize);
        assertTrue(status.stats.totalMatchedSize() == 0);
        ReturnStatus status2 = fileCopy(false, src, dst);
        assertTrue(status2.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status2.stats.numFiles() == numDirs + numFiles);
        assertTrue(status2.stats.numTransferredFiles() == numFiles);
        assertTrue(status2.stats.totalLiteralSize() == 0);
        assertTrue(status2.stats.totalMatchedSize() == fileSize);
    }

    @Test
    public void testCopyFileTwiceNotMultipleBlockSizeTimes()
        throws IOException
    {
        Path src = _tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 557;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0x18, fileSize);
        FileUtil.writeToFiles(content, src);
        Files.setLastModifiedTime(src, FileTime.fromMillis(0));
        ReturnStatus status = fileCopy(src, dst, "--times");
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.numFiles() == numDirs + numFiles);
        assertTrue(status.stats.numTransferredFiles() == numFiles);
        assertTrue(status.stats.totalLiteralSize() == fileSize);
        assertTrue(status.stats.totalMatchedSize() == 0);
        ReturnStatus status2 = fileCopy(false, src, dst);
        assertTrue(status2.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status2.stats.numFiles() == numDirs + numFiles);
        assertTrue(status2.stats.numTransferredFiles() == 0);
        assertTrue(status2.stats.totalLiteralSize() == 0);
        assertTrue(status2.stats.totalMatchedSize() == 0);
    }

    @Test
    public void testClientCopyPreserveUid() throws IOException
    {
        if (!User.JVM_USER.equals(User.ROOT)) {
            return;
        }

        Path src = _tempDir.newFolder().toPath();
        Path dst = Paths.get(src.toString() + ".dst");

        Path srcDir = src.resolve("dir");
        Path srcFile = srcDir.resolve("file");
        Files.createDirectory(srcDir);
        FileUtil.writeToFiles(1, srcFile);
        FileOps.setUserId(srcFile, User.NOBODY.id());

        Files.createDirectory(dst);
        Path copyOfSrc = dst.resolve(src.getFileName());
        Files.createDirectory(copyOfSrc);
        Path dstDir = copyOfSrc.resolve("dir");
        Path dstFile = dstDir.resolve("file");
        Files.createDirectory(dstDir);
        FileUtil.writeToFiles(1, dstFile);

        ReturnStatus status = fileCopy(src, dst, "--recursive", "--owner",
                                       "--numeric-ids");

        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isDirectory(dst));
        assertTrue(FileUtil.isDirectoriesIdentical(src, copyOfSrc));
        assertTrue(FileUtil.isFileSameOwner(RsyncFileAttributes.stat(srcFile),
                                            RsyncFileAttributes.stat(dstFile)));
    }

    @Test
    public void testClientCopyPreserveGid() throws IOException
    {
        if (!User.JVM_USER.equals(User.ROOT)) {
            return;
        }

        Path src = _tempDir.newFolder().toPath();
        Path dst = Paths.get(src.toString() + ".dst");

        Path srcDir = src.resolve("dir");
        Path srcFile = srcDir.resolve("file");
        Files.createDirectory(srcDir);
        FileUtil.writeToFiles(1, srcFile);
        FileOps.setGroupId(srcFile, User.NOBODY.id());

        Files.createDirectory(dst);
        Path copyOfSrc = dst.resolve(src.getFileName());
        Files.createDirectory(copyOfSrc);
        Path dstDir = copyOfSrc.resolve("dir");
        Path dstFile = dstDir.resolve("file");
        Files.createDirectory(dstDir);
        FileUtil.writeToFiles(1, dstFile);

        ReturnStatus status = fileCopy(src, dst, "--recursive", "--group",
                                       "--numeric-ids");

        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isDirectory(dst));
        assertTrue(FileUtil.isDirectoriesIdentical(src, copyOfSrc));
        assertTrue(FileUtil.isFileSameGroup(RsyncFileAttributes.stat(srcFile),
                                            RsyncFileAttributes.stat(dstFile)));
    }

    @Test(timeout=100)
    public void testServerHelp() throws InterruptedException, IOException
    {
        int rc = newServer(new TestModules()).
            setStandardOut(_nullOut).
            start(new String[] { "--help" });
        assertTrue(rc == 0);
    }

    // FIXME: latch might not get decreased if exception occurs
    // FIXME: port might be unavailable, open it here and inject it
    @Test(timeout=100)
    public void testServerConnection() throws InterruptedException
    {
        final CountDownLatch isListeningLatch = new CountDownLatch(1);

        Callable<Integer> serverTask = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception
            {
                Path modulePath = _tempDir.newFolder().toPath();
                Module m = new SimpleModule(Paths.get("test"), modulePath,
                                            "a test module", true, false);
                int rc = newServer(new TestModules(m)).
                        setIsListeningLatch(isListeningLatch).
                        start(new String[] { "--port=14415" });
                return rc;
            }
        };
        _service.submit(serverTask);
        isListeningLatch.await();
        SyncClient client = newClient();
        int rc = client.start(new String[] { "--port=14415", "localhost::" });
        assertTrue(rc == 0);
    }

    @Test(timeout=1000)
    public void testProtectedServerConnection()
            throws InterruptedException, IOException
    {
        final CountDownLatch isListeningLatch = new CountDownLatch(1);
        final String restrictedModuleName = "Restricted";
        final String authToken = "ëẗÿåäöüﭏ사غ";
        Callable<Integer> serverTask = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception
            {
                Path modulePath = _tempDir.newFolder().toPath();
                Module m = new SimpleModule(Paths.get(restrictedModuleName),
                                            modulePath,
                                            "a test module", true, false);
                RestrictedModule rm = new SimpleRestrictedModule(
                                                 authToken,
                                                 m,
                                                 restrictedModuleName,
                                                 "a restricted module");
                int rc = newServer(new TestModules(rm)).
                        setIsListeningLatch(isListeningLatch).
                        start(new String[] { "--port=14415" });
                return rc;
            }
        };
        _service.submit(serverTask);
        isListeningLatch.await();
        SyncClient client = newClient();

        // System.setIn(new ByteArrayInputStream(authToken.getBytes()));
        int rc = client.start(new String[] { "--port=14415",
                "--password-file=" + FileUtil.createPasswordFile(authToken)
                        .getAbsolutePath(),
                "localhost::" + restrictedModuleName });
        assertTrue(rc == 0);
    }

    @Test(timeout=1000)
    public void testInvalidPassword()
 throws InterruptedException, IOException
    {
        final CountDownLatch isListeningLatch = new CountDownLatch(1);
        final String restrictedModuleName = "Restricted";
        final String authToken = "testAuthToken";

        Callable<Integer> serverTask = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception
            {
                Path modulePath = _tempDir.newFolder().toPath();
                Module m = new SimpleModule(Paths.get(restrictedModuleName),
                                            modulePath,
                                            "a test module", true, false);
                RestrictedModule rm = new SimpleRestrictedModule(
                                                 authToken,
                                                 m,
                                                 restrictedModuleName,
                                                 "a restricted module");
                int rc = newServer(new TestModules(rm)).
                        setIsListeningLatch(isListeningLatch).
                        start(new String[] { "--port=14415" });
                return rc;
            }
        };
        _service.submit(serverTask);
        isListeningLatch.await();
        SyncClient client = newClient();

        // System.setIn(new ByteArrayInputStream((authToken +
        // "fail").getBytes()));
        int rc = client
                .start(new String[] { "--port=14415",
                        "--password-file=" + FileUtil
                                .createPasswordFile(authToken + "fail")
                                .getAbsolutePath(),
                "localhost::" + restrictedModuleName });
        assertTrue(rc != 0);
    }
}
