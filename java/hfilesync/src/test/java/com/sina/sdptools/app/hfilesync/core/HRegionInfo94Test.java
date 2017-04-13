package com.sina.sdptools.app.hfilesync.core;

import com.google.common.base.Charsets;
import com.sina.sdptools.app.hfilesync.core.HRegionInfo94;
import com.sina.sdptools.app.hfilesync.core.MoreIOUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

public class HRegionInfo94Test {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void testReadFields() throws IOException {
        InputStream in = HRegionInfo94Test.class.getResourceAsStream("regioninfo");
        try {
            HRegionInfo94 ri = new HRegionInfo94();
            ri.readFields(new DataInputStream(in));
            assertEquals("00141c192012ced16d54835996db80e9", ri.getEncodedName());
            assertEquals("1042018:837ed784570c5aa00b18b5d736eb02d2", new String(ri.getStartKey(), Charsets.UTF_8));
            assertEquals("1042018:838624c5d16c075f3c9e01bea363417c", new String(ri.getEndKey(), Charsets.UTF_8));
        } finally {
            MoreIOUtils.closeQuietly(in);
        }
    }
}
