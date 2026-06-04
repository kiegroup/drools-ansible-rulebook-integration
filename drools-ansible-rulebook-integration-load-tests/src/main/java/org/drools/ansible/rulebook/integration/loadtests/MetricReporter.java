package org.drools.ansible.rulebook.integration.loadtests;

import java.io.PrintStream;

public final class MetricReporter {

    private MetricReporter() {}

    public static void report(PrintStream err, String eventsJson, boolean haPg,
                              boolean failoverRecovery,
                              long usedMemoryBytes, long timeMs) {
        StringBuilder sb = new StringBuilder();
        sb.append(eventsJson);
        if (haPg) {
            sb.append(" (HA-PG)");
        }
        if (failoverRecovery) {
            sb.append(" (failover-recovery)");
        }
        sb.append(", ").append(usedMemoryBytes);
        sb.append(", ").append(timeMs);
        err.println(sb.toString());
    }
}
