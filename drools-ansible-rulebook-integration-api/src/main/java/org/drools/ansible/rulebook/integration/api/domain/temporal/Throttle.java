package org.drools.ansible.rulebook.integration.api.domain.temporal;

import java.util.List;

public class Throttle {

    private String onceWithin;
    private String onceAfter;
    private List<String> groupByAttributes;

    public void setOnce_within(String onceWithin) {
        this.onceWithin = onceWithin;
    }

    public void setOnce_after(String onceAfter) {
        this.onceAfter = onceAfter;
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
        throw new IllegalArgumentException("Invalid throttle definition");
    }
}
