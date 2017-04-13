package com.sina.sdptools.app.hfilesync.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class JobNamer {

    private final String prefixName;
    private final List<Integer> prefixStages;
    private final AtomicInteger nextId = new AtomicInteger();

    public JobNamer(String prefixName) {
        this.prefixName = prefixName;
        this.prefixStages = Collections.emptyList();
    }

    private JobNamer(String prefixName, List<Integer> prefixStages) {
        this.prefixName = prefixName;
        this.prefixStages = prefixStages;
    }

    public String nextJobName() {
        StringBuilder sb = new StringBuilder();
        sb.append(prefixName);
        sb.append(" (stage ");
        for (int stage : prefixStages) {
            sb.append(stage);
            sb.append("-");
        }
        sb.append(nextId.getAndIncrement());
        sb.append(")");
        return sb.toString();
    }

    public JobNamer newSubStage() {
        List<Integer> p = new ArrayList<Integer>(prefixStages);
        p.add(nextId.getAndIncrement());
        return new JobNamer(prefixName, p);
    }
}
