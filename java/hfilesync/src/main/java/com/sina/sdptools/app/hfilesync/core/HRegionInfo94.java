/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sina.sdptools.app.hfilesync.core;

import com.google.common.base.Charsets;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;

/**
 * 从 HBase 0.94 代码中抽出来的，只保留读取功能
 */
public class HRegionInfo94 {

    private static final byte VERSION = 1;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * Separator used to demarcate the encodedName in a region name
     * in the new format. See description on new format above.
     */
    private static final int ENC_SEPARATOR = '.';
    public static final int MD5_HEX_LENGTH = 32;

    /**
     * A non-capture group so that this can be embedded.
     */
    public static final String ENCODED_REGION_NAME_REGEX = "(?:[a-f0-9]+)";

    /**
     * Does region name contain its encoded name?
     *
     * @param regionName region name
     * @return boolean indicating if this a new format region
     * name which contains its encoded name.
     */
    private static boolean hasEncodedName(final byte[] regionName) {
        // check if region name ends in ENC_SEPARATOR
        if ((regionName.length >= 1)
                && (regionName[regionName.length - 1] == ENC_SEPARATOR)) {
            // region name is new format. it contains the encoded name.
            return true;
        }
        return false;
    }

    /**
     * @param regionName
     * @return the encodedName
     */
    public static String encodeRegionName(final byte[] regionName) {
        String encodedName;
        if (!hasEncodedName(regionName)) {
            throw new AssertionError("old format is not supported");
        }
        // region is in new format:
        // <tableName>,<startKey>,<regionIdTimeStamp>/encodedName/
        encodedName = bytesToString(regionName,
                regionName.length - MD5_HEX_LENGTH - 1,
                MD5_HEX_LENGTH);
        return encodedName;
    }

    /**
     * Use logging.
     *
     * @param encodedRegionName The encoded regionname.
     * @return <code>-ROOT-</code> if passed <code>70236052</code> or
     * <code>.META.</code> if passed </code>1028785192</code> else returns
     * <code>encodedRegionName</code>
     */
    public static String prettyPrint(final String encodedRegionName) {
        if (encodedRegionName.equals("70236052")) {
            return encodedRegionName + "/-ROOT-";
        } else if (encodedRegionName.equals("1028785192")) {
            return encodedRegionName + "/.META.";
        }
        return encodedRegionName;
    }

    /**
     * delimiter used between portions of a region name
     */
    public static final int DELIMITER = ',';

    private byte[] endKey = EMPTY_BYTE_ARRAY;
    // This flag is in the parent of a split while the parent is still referenced
    // by daughter regions.  We USED to set this flag when we disabled a table
    // but now table state is kept up in zookeeper as of 0.90.0 HBase.
    private boolean offLine = false;
    private long regionId = -1;
    private transient byte[] regionName = EMPTY_BYTE_ARRAY;
    private String regionNameStr = "";
    private boolean split = false;
    private byte[] startKey = EMPTY_BYTE_ARRAY;
    private int hashCode = -1;
    //TODO: Move NO_HASH to HStoreFile which is really the only place it is used.
    public static final String NO_HASH = null;
    private volatile String encodedName = NO_HASH;
    private byte[] encodedNameAsBytes = null;

    // Current TableName
    private byte[] tableName = null;

    private void setHashCode() {
        int result = Arrays.hashCode(this.regionName);
        result ^= this.regionId;
        result ^= Arrays.hashCode(this.startKey);
        result ^= Arrays.hashCode(this.endKey);
        result ^= Boolean.valueOf(this.offLine).hashCode();
        result ^= Arrays.hashCode(this.tableName);
        this.hashCode = result;
    }

    /**
     * Gets the table name from the specified region name.
     *
     * @param regionName
     * @return Table name.
     */
    public static byte[] getTableName(byte[] regionName) {
        int offset = -1;
        for (int i = 0; i < regionName.length; i++) {
            if (regionName[i] == DELIMITER) {
                offset = i;
                break;
            }
        }
        byte[] tableName = new byte[offset];
        System.arraycopy(regionName, 0, tableName, 0, offset);
        return tableName;
    }

    /**
     * Gets the start key from the specified region name.
     *
     * @param regionName
     * @return Start key.
     */
    public static byte[] getStartKey(final byte[] regionName) throws IOException {
        return parseRegionName(regionName)[1];
    }

    /**
     * Separate elements of a regionName.
     *
     * @param regionName
     * @return Array of byte[] containing tableName, startKey and id
     * @throws IOException
     */
    public static byte[][] parseRegionName(final byte[] regionName)
            throws IOException {
        int offset = -1;
        for (int i = 0; i < regionName.length; i++) {
            if (regionName[i] == DELIMITER) {
                offset = i;
                break;
            }
        }
        if (offset == -1) throw new IOException("Invalid regionName format");
        byte[] tableName = new byte[offset];
        System.arraycopy(regionName, 0, tableName, 0, offset);
        offset = -1;
        for (int i = regionName.length - 1; i > 0; i--) {
            if (regionName[i] == DELIMITER) {
                offset = i;
                break;
            }
        }
        if (offset == -1) throw new IOException("Invalid regionName format");
        byte[] startKey = EMPTY_BYTE_ARRAY;
        if (offset != tableName.length + 1) {
            startKey = new byte[offset - tableName.length - 1];
            System.arraycopy(regionName, tableName.length + 1, startKey, 0,
                    offset - tableName.length - 1);
        }
        byte[] id = new byte[regionName.length - offset - 1];
        System.arraycopy(regionName, offset + 1, id, 0,
                regionName.length - offset - 1);
        byte[][] elements = new byte[3][];
        elements[0] = tableName;
        elements[1] = startKey;
        elements[2] = id;
        return elements;
    }

    /**
     * @return the regionId
     */
    public long getRegionId() {
        return regionId;
    }

    /**
     * @return the regionName as an array of bytes.
     * @see #getRegionNameAsString()
     */
    public byte[] getRegionName() {
        return regionName;
    }

    /**
     * @return Region name as a String for use in logging, etc.
     */
    public String getRegionNameAsString() {
        if (hasEncodedName(this.regionName)) {
            // new format region names already have their encoded name.
            return this.regionNameStr;
        }

        // old format. regionNameStr doesn't have the region name.
        //
        //
        return this.regionNameStr + "." + this.getEncodedName();
    }

    /**
     * @return the encoded region name
     */
    public synchronized String getEncodedName() {
        if (this.encodedName == NO_HASH) {
            this.encodedName = encodeRegionName(this.regionName);
        }
        return this.encodedName;
    }

    public synchronized byte[] getEncodedNameAsBytes() {
        if (this.encodedNameAsBytes == null) {
            this.encodedNameAsBytes = getEncodedName().getBytes(Charsets.UTF_8);
        }
        return this.encodedNameAsBytes;
    }

    /**
     * @return the startKey
     */
    public byte[] getStartKey() {
        return startKey;
    }

    /**
     * @return the endKey
     */
    public byte[] getEndKey() {
        return endKey;
    }

    /**
     * Get current table name of the region
     *
     * @return byte array of table name
     */
    public byte[] getTableName() {
        if (tableName == null || tableName.length == 0) {
            tableName = getTableName(getRegionName());
        }
        return tableName;
    }

    /**
     * @return True if has been split and has daughters.
     */
    public boolean isSplit() {
        return this.split;
    }

    /**
     * @param split set split status
     */
    public void setSplit(boolean split) {
        this.split = split;
    }

    /**
     * @return True if this region is offline.
     */
    public boolean isOffline() {
        return this.offLine;
    }

    public void readFields(DataInput in) throws IOException {
        byte version = in.readByte();
        if (version != VERSION) {
            throw new IOException("Unsupport version; " + (version & 0xFF));
        }
        this.endKey = readByteArray(in);
        this.offLine = in.readBoolean();
        this.regionId = in.readLong();
        this.regionName = readByteArray(in);
        this.regionNameStr = toStringBinary(this.regionName);
        this.split = in.readBoolean();
        this.startKey = readByteArray(in);
        this.tableName = readByteArray(in);
        this.hashCode = in.readInt();
    }

    // 工具方法，从 o.a.hadoop.hbase.util.Bytes 抽出来的
    private static byte[] readByteArray(DataInput in) throws IOException {
        int len = WritableUtils.readVInt(in);
        if (len < 0) {
            throw new NegativeArraySizeException(Integer.toString(len));
        }
        byte[] result = new byte[len];
        in.readFully(result, 0, len);
        return result;
    }

    // 工具方法，从 o.a.hadoop.hbase.util.Bytes 抽出来的
    private static String toStringBinary(final byte[] b) {
        if (b == null)
            return "null";
        return toStringBinary(b, 0, b.length);
    }

    // 工具方法，从 o.a.hadoop.hbase.util.Bytes 抽出来的
    private static String toStringBinary(final byte[] b, int off, int len) {
        StringBuilder result = new StringBuilder();
        // Just in case we are passed a 'len' that is > buffer length...
        if (off >= b.length) return result.toString();
        if (off + len > b.length) len = b.length - off;
        for (int i = off; i < off + len; ++i) {
            int ch = b[i] & 0xFF;
            if ((ch >= '0' && ch <= '9')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= 'a' && ch <= 'z')
                    || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0) {
                result.append((char) ch);
            } else {
                result.append(String.format("\\x%02X", ch));
            }
        }
        return result.toString();
    }

    // 工具方法，从 o.a.hadoop.hbase.util.Bytes 抽出来的
    private static String bytesToString(final byte[] b) {
        if (b == null) {
            return null;
        }
        return bytesToString(b, 0, b.length);
    }

    // 工具方法，从 o.a.hadoop.hbase.util.Bytes 抽出来的
    private static String bytesToString(final byte[] b, int off, int len) {
        if (b == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        return new String(b, off, len, Charsets.UTF_8);
    }
}
