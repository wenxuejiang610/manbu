package com.sina.sdptools.app.hfilesync.core;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public final class MorePreconditions {

    private MorePreconditions() {}

    public static String checkNotNullOrEmpty(String s) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(s));
        return s;
    }
}
