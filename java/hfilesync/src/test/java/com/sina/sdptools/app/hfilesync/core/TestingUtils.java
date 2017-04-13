package com.sina.sdptools.app.hfilesync.core;

import com.google.common.base.Charsets;
import com.sina.sdptools.app.hfilesync.core.MoreIOUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hamcrest.Matcher;
import org.mockito.ArgumentMatcher;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

public final class TestingUtils {

    private static final byte[] HEX_DIGITS = "0123456789abcdef".getBytes(Charsets.UTF_8);
    private static final Random RANDOM = new Random();

    private TestingUtils() {}

    public static void createFile(FileSystem fs , Path path, String content) throws IOException {
        createFile(fs, path, content.getBytes(Charsets.UTF_8));
    }

    public static void createFile(FileSystem fs , Path path, byte[] content) throws IOException {
        fs.mkdirs(path.getParent());
        OutputStream out = fs.create(path, /* overwrite */ true);
        try {
            out.write(content);
            out.flush();
        } finally {
            MoreIOUtils.closeQuietly(out);
        }
    }

    public static byte[] readResourceAsBytes(Class<?> clazz, String resource) throws IOException {
        InputStream in = clazz.getResourceAsStream(resource);
        try {
            return MoreIOUtils.readFully(in);
        } finally {
            MoreIOUtils.closeQuietly(in);
        }
    }

    public static void assertFileContent(FileSystem fs, Path path, String content) throws IOException {
        assertFileContent(fs, path, content.getBytes(Charsets.UTF_8));
    }

    public static void assertFileContent(FileSystem fs, Path path, byte[] content) throws IOException {
        InputStream in = fs.open(path);
        try {
            byte[] b = MoreIOUtils.readFully(in);
            assertArrayEquals(content, b);
        } finally {
            MoreIOUtils.closeQuietly(in);
        }
    }

    public static void assertFileNotExist(FileSystem fs, Path path) throws IOException {
        assertFalse(fs.exists(path));
    }

    public static String randomHFileOrRegionName() {
        StringBuilder sb = new StringBuilder(32);
        for (int i = 0; i < 32; i++) {
            sb.append((char) HEX_DIGITS[RANDOM.nextInt(16)]);
        }
        return sb.toString();
    }

    public static <T> Matcher<List<T>> hasSizeOf(final int size) {
        return new ArgumentMatcher<List<T>>() {
            @Override
            public boolean matches(Object arg) {
                if (arg instanceof List) {
                    return ((List) arg).size() == size;
                } else {
                    return false;
                }
            }
        };
    }
}
