package org.drools.ansible.rulebook.integration.api.rulesengine;

public class MemoryThresholdReachedException extends RuntimeException {

    private final int threshold;
    private final int actual;

    public MemoryThresholdReachedException(int threshold, int actual) {
        this.threshold = threshold;
        this.actual = actual;
    }

    @Override
    public String getMessage() {
        return "Memory threshold reached: " + actual + "% > " + threshold + "%";
    }
}