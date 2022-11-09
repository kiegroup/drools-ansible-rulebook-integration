package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.ArrayList;
import java.util.List;

import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeVariable;
import org.drools.model.view.ViewItem;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.TimeAmount.parseTimeAmount;
import static org.drools.model.DSL.after;

/**
 * Related events have to match within a time window
 *
 *    e.g.:
 *      condition:
 *         all:
 *           - events.ping << event.ping.timeout == true   # no host info
 *           - events.process << event.sensu.process.status == "stopped"   # web server
 *           - events.database << event.sensu.storage.percent > 95  # database server
 *         time_window: 5 minutes
 *
 * This feature has been implemented synthetically adding temporal constraints to the events patterns so for example this
 * condition is internally rewritten as it follows:
 *
 * - events.ping << event.ping.timeout == true
 * - events.process << event.sensu.process.status == "stopped"
 *       && this after [-5, TimeUnit.MINUTE, 5, TimeUnit.MINUTE] events.ping
 * - events.database << event.sensu.storage.percent > 95  # database server
 *       && this after [-5, TimeUnit.MINUTE, 5, TimeUnit.MINUTE] events.ping
 *       && this after [-5, TimeUnit.MINUTE, 5, TimeUnit.MINUTE] events.process
 *
 * Note that the use of a negative range in the after constraint allows the matching of this rule also when the events arrive
 * in an order that is different from the one listed in the rule itself.
 */
public class TimeWindowDefinition implements TimeConstraint {

    private final TimeAmount timeAmount;

    private final List<PrototypeVariable> formerVariables = new ArrayList<>();

    private TimeWindowDefinition(TimeAmount timeAmount) {
        this.timeAmount = timeAmount;
    }

    static TimeWindowDefinition parseTimeWindow(String timeWindow) {
        return new TimeWindowDefinition(parseTimeAmount(timeWindow));
    }

    public ViewItem appendTimeConstraint(ViewItem pattern) {
        PrototypeDSL.PrototypePatternDef protoPattern = (PrototypeDSL.PrototypePatternDef) pattern;
        formerVariables.forEach(v -> protoPattern.expr(after(-timeAmount.getAmount(), timeAmount.getTimeUnit(), timeAmount.getAmount(), timeAmount.getTimeUnit()), v));
        formerVariables.add((PrototypeVariable) protoPattern.getFirstVariable());
        return protoPattern;
    }
}