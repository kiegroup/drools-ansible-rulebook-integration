package org.drools.ansible.rulebook.integration.loadtests;

import java.util.List;
import java.util.Map;

public final class Result {

    public final List<Map> matches;
    public final long durationMs;
    public final long usedMemoryBytes;

    public Result(List<Map> matches, long durationMs, long usedMemoryBytes) {
        this.matches = matches;
        this.durationMs = durationMs;
        this.usedMemoryBytes = usedMemoryBytes;
    }
}
