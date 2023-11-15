package org.drools.ansible.rulebook.integration.api.rulesengine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryMonitorUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryMonitorUtil.class.getName());

    public static final String MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD_PROPERTY = "drools.memory.occupation.percentage.threshold";
    private static final int DEFAULT_MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD = 90;
    private static final int MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD;

    // check memory per configured number of events are consumed
    public static final String MEMORY_CHECK_EVENT_COUNT_THRESHOLD_PROPERTY = "drools.memory.check.event.count.threshold";
    private static final int DEFAULT_MEMORY_CHECK_EVENT_COUNT_THRESHOLD = 64;
    private static final int MEMORY_CHECK_EVENT_COUNT_MASK;
    private static int COUNTER = 0;

    static {
        String memoryThresholdEnvValue = System.getenv("DROOLS_MEMORY_THRESHOLD");
        if (memoryThresholdEnvValue != null && !memoryThresholdEnvValue.isEmpty()) {
            // Environment variable takes precedence over system property
            System.setProperty(MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD_PROPERTY, memoryThresholdEnvValue);
        }
        MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD = Integer.getInteger(MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD_PROPERTY, DEFAULT_MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD); // percentage
        LOG.info("Memory occupation threshold set to {}%", MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD);

        String eventCountThresholdEnvValue = System.getenv("DROOLS_MEMORY_CHECK_EVENT_COUNT_THRESHOLD");
        if (eventCountThresholdEnvValue != null && !eventCountThresholdEnvValue.isEmpty()) {
            // Environment variable takes precedence over system property
            System.setProperty(MEMORY_CHECK_EVENT_COUNT_THRESHOLD_PROPERTY, eventCountThresholdEnvValue);
        }
        
        int eventCountThreshold = Integer.getInteger(MEMORY_CHECK_EVENT_COUNT_THRESHOLD_PROPERTY, DEFAULT_MEMORY_CHECK_EVENT_COUNT_THRESHOLD); // number of events
        MEMORY_CHECK_EVENT_COUNT_MASK = roundToPowerOfTwo(eventCountThreshold) - 1;
        LOG.info("Memory check event count threshold set to {}", MEMORY_CHECK_EVENT_COUNT_MASK);
    }

    private MemoryMonitorUtil() {
        // do not instantiate
    }

    public static void checkMemoryOccupation() {
        if ((COUNTER++ & MEMORY_CHECK_EVENT_COUNT_MASK) != 0) {
            // check memory occupation only once in 64 calls
            return;
        }
        int memoryOccupationPercentage = getMemoryOccupationPercentage();
        if (memoryOccupationPercentage > MEMORY_OCCUPATION_PERCENTAGE_THRESHOLD) {
            // give GC a chance to free some memory
            System.gc(); // NOSONAR
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

    private static int roundToPowerOfTwo(final int value) {
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("There is no larger power of 2 int for value:" + value + " since it exceeds 2^31.");
        }
        if (value < 0) {
            throw new IllegalArgumentException("Given value:" + value + ". Expecting value >= 0.");
        }
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }
}
