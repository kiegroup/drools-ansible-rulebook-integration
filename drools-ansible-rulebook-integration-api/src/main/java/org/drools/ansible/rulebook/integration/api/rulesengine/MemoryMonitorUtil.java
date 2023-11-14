package org.drools.ansible.rulebook.integration.api.rulesengine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryMonitorUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryMonitorUtil.class.getName());

    public static final String MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD_PROPERTY = "drools.memory.occupation.percentage.threshold";

    private static final int DEFAULT_MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD = 90;

    private static final int MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD;

    static {
        String envValue = System.getenv("DROOLS_MEMORY_THRESHOLD");
        if (envValue != null && !envValue.isEmpty()) {
            // Environment variable takes precedence over system property
            System.setProperty(MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD_PROPERTY, envValue);
        }
        MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD = Integer.getInteger(MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD_PROPERTY, DEFAULT_MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD); // percentage
    }

    private MemoryMonitorUtil() {
        // do not instantiate
    }

    public static void checkMemoryOccupation() {
        int memoryOccupationPercentage = getMemoryOccupationPercentage();
        if (memoryOccupationPercentage > MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD) {
            System.gc(); // NOSONAR
            // double check to avoid frequent GC
            memoryOccupationPercentage = getMemoryOccupationPercentage();
            if (memoryOccupationPercentage > MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD) {
                LOG.error("Memory occupation is above the threshold: {}% > {}%. MaxMemory = {}, UsedMemory = {}",
                          memoryOccupationPercentage, MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD, Runtime.getRuntime().maxMemory(), Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
                throw new MemoryThresholdReachedException(MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD, memoryOccupationPercentage);
            }
        }
    }

    private static int getMemoryOccupationPercentage() {
        return (int) ((100 * (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())) / Runtime.getRuntime().maxMemory());
    }
}
