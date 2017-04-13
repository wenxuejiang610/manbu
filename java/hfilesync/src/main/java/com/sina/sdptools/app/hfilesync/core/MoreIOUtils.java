package com.sina.sdptools.app.hfilesync.core;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** IO 相关工具方法，叫 {@code MoreIOUitls} 以免和人多 {@code IOUtils} 重名，同时避免引入 {@code commons-io} 包。 */
public final class MoreIOUtils {

    private MoreIOUtils() {}

    public static void closeQuietly(@Nullable Closeable c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (IOException ignored) {
        }
    }

    public static long copy(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[4096];
        long bytes = 0;
        int n;
        while ((n = in.read(buffer)) > 0) {
            out.write(buffer, 0, n);
            bytes += n;
        }
        return bytes;
    }

    public static byte[] readFully(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        copy(in, out);
        return out.toByteArray();
    }
}
