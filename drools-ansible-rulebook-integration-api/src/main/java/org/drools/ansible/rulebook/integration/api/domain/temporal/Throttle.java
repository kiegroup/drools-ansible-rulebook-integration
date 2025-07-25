package org.drools.ansible.rulebook.integration.api.domain.temporal;

import java.util.List;

public class Throttle {

    private String onceWithin;
    private String onceAfter;
    private String accumulateWithin;
    private Integer threshold;
    private List<String> groupByAttributes;

    public void setOnce_within(String onceWithin) {
        this.onceWithin = onceWithin;
    }

    public void setOnce_after(String onceAfter) {
        this.onceAfter = onceAfter;
    }

    public void setAccumulate_within(String accumulateWithin) {
        this.accumulateWithin = accumulateWithin;
    }

    public void setThreshold(Integer threshold) {
        this.threshold = threshold;
    }

    public void setGroup_by_attributes(List<String> groupByAttributes) {
        this.groupByAttributes = groupByAttributes;
    }

    public TimeConstraint asTimeConstraint() {
        if (onceWithin != null) {
            return OnceWithinDefinition.parseOnceWithin(onceWithin, groupByAttributes);
        }
        if (onceAfter != null) {
            return OnceAfterDefinition.parseOnceAfter(onceAfter, groupByAttributes);
        }
        if (accumulateWithin != null) {
            if (threshold == null) {
                throw new IllegalArgumentException("threshold is required for accumulate_within");
            }
            return AccumulateWithinDefinition.parseAccumulateWithin(accumulateWithin, threshold, groupByAttributes);
        }
        throw new IllegalArgumentException("Invalid throttle definition");
    }
}
