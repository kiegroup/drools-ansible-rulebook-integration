package org.drools.ansible.rulebook.integration.api.domain.temporal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.model.Drools;
import org.drools.model.Rule;
import org.drools.model.Variable;
import org.drools.model.prototype.PrototypeDSL;
import org.drools.model.view.CombinedExprViewItem;
import org.drools.model.view.ViewItem;
import org.kie.api.prototype.PrototypeEventInstance;
import org.kie.api.prototype.PrototypeFactInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.rulesengine.RegisterOnlyAgendaFilter.SYNTHETIC_RULE_TAG;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototypeEvent;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.writeMetaDataOnEvent;
import static org.drools.model.DSL.not;
import static org.drools.model.DSL.on;
import static org.drools.model.Index.ConstraintType;
import static org.drools.model.PatternDSL.rule;

/**
 * Accumulate events within a time window until a threshold is met: when the threshold is reached,
 * the rule fires immediately with the event that triggered the threshold. After firing, the accumulation
 * resets and starts again with the next event. If the time window expires before the threshold is met,
 * the accumulated events are silently discarded.
 *
 *    e.g.:
 *      condition:
 *         all:
 *           - singleton << event.sensu.process.type == "alert"
 *         throttle:
 *           accumulate_within: 10 minutes
 *           threshold: 3
 *           group_by_attributes:
 *             - event.sensu.host
 *             - event.sensu.process.type
 *
 * It has been implemented by inserting a synthetic control event when the first event arrives,
 * which tracks the count of accumulated events. The control event expires after the time window.
 * When the threshold is reached, the main rule fires with the triggering event, and the control
 * event is deleted to reset the accumulation.
 *
 * In other words, the former example is translated into the following 3 rules:
 *
 * rule R when
 *   singleton : Event( sensu.process.type == "alert" )
 *   control : Control( sensu.host == singleton.sensu.host, sensu.process.type == singleton.sensu.process.type,
 *                     drools_rule_name == "R", current_count >= 3 )
 * then
 *   // original consequence
 *   delete singleton; // delete the triggering event
 *   delete control event
 *
 * rule R_first_event when
 *   singleton : Event( sensu.process.type == "alert" )
 *   not Control( sensu.host == singleton.sensu.host, sensu.process.type == singleton.sensu.process.type,
 *               drools_rule_name == "R" )
 * then
 *   insert Control( sensu.host = singleton.sensu.host, sensu.process.type = singleton.sensu.process.type,
 *                  drools_rule_name = "R", current_count = 0, @expires( 10m ) )
 *
 * rule R_accumulate when
 *   singleton : Event( sensu.process.type == "alert" )
 *   control : Control( sensu.host == singleton.sensu.host, sensu.process.type == singleton.sensu.process.type,
 *                     drools_rule_name == "R", current_count < 3 )
 * then
 *   control.current_count++
 *   if (control.current_count < 3) delete singleton
 */
public class AccumulateWithinDefinition extends OnceAbstractTimeConstraint {

    private static final Logger log = LoggerFactory.getLogger(AccumulateWithinDefinition.class);

    public static final String KEYWORD = "accumulate_within";

    private final int threshold;

    public AccumulateWithinDefinition(TimeAmount timeAmount, int threshold, List<GroupByAttribute> groupByAttributes) {
        super(timeAmount, groupByAttributes);
        this.threshold = threshold;
    }

    @Override
    public boolean requiresAsyncExecution() {
        return false;
    }

    public String getKeyword() {
        return KEYWORD;
    }

    @Override
    public ViewItem processTimeConstraint(String ruleName, ViewItem pattern) {
        this.ruleName = ruleName;
        if (guardedPattern != null) {
            throw new IllegalStateException("Cannot process this TimeConstraint twice");
        }
        guardedPattern = (PrototypeDSL.PrototypePatternDef) pattern;
        // For accumulate_within, the main rule should only fire when there's a control event
        // with current_count >= threshold
        PrototypeDSL.PrototypePatternDef thresholdMetPattern = createControlPattern()
                .expr("current_count", ConstraintType.GREATER_OR_EQUAL, threshold);
        return new CombinedExprViewItem(org.drools.model.Condition.Type.AND,
                                        new ViewItem[]{guardedPattern, thresholdMetPattern});
    }

    @Override
    public Variable<?>[] getTimeConstraintConsequenceVariables() {
        return new Variable[]{getPatternVariable(), getControlVariable()};
    }

    @Override
    public void executeTimeConstraintConsequence(Drools drools, Object... facts) {
        // When main rule fires, add metadata to the event and clean up control event
        PrototypeFactInstance fact = (PrototypeFactInstance) facts[0];
        Map<String, Object> ruleEngineMeta = new HashMap<>();
        ruleEngineMeta.put("accumulate_within_time_window", timeAmount.toString());
        ruleEngineMeta.put("threshold", threshold);
        writeMetaDataOnEvent(fact, ruleEngineMeta);

        // Delete the event
        drools.delete(facts[0]);

        // Delete the control event
        drools.delete(facts[1]);
    }

    @Override
    public List<Rule> getControlRules(RuleGenerationContext ruleContext) {
        List<Rule> rules = new ArrayList<>();

        // Rule 1: First event handler
        rules.add(
                rule(ruleName + "_first_event").metadata(SYNTHETIC_RULE_TAG, true)
                        .build(
                                guardedPattern,
                                not(createControlPattern()),
                                on(getPatternVariable()).execute((drools, event) -> {
                                    // Create control event for accumulation
                                    PrototypeEventInstance controlEvent = getPrototypeEvent(SYNTHETIC_PROTOTYPE_NAME).newInstance()
                                            .withExpiration(timeAmount.getAmount(), timeAmount.getTimeUnit());

                                    // Set group-by attributes
                                    for (GroupByAttribute unique : groupByAttributes) {
                                        controlEvent.put(unique.getKey(), unique.evalExtractorOnFact(event));
                                    }

                                    controlEvent.put("drools_rule_name", ruleName);
                                    controlEvent.put("current_count", 0); // the count will be incremented in the accumulation rule
                                    drools.insert(controlEvent);
                                })
                        )
        );

        // Rule 2: Accumulation handler
        rules.add(
                rule(ruleName + "_accumulate").metadata(SYNTHETIC_RULE_TAG, true)
                        .build(
                                guardedPattern,
                                createControlPattern().expr("current_count", ConstraintType.LESS_THAN, threshold),
                                on(getPatternVariable(), getControlVariable()).execute((drools, event, control) -> {
                                    int newCount = ((int) control.get("current_count")) + 1;
                                    control.put("current_count", newCount);
                                    drools.update(control);

                                    if (newCount < threshold) {
                                        // If the count is still below threshold, the event is discarded
                                        drools.delete(event);
                                    }
                                })
                        )
        );

        return rules;
    }

    @Override
    public String toString() {
        return "AccumulateWithinDefinition{" +
                "timeAmount=" + timeAmount +
                ", threshold=" + threshold +
                ", groupByAttributes=" + groupByAttributes +
                "}";
    }

    public static AccumulateWithinDefinition parseAccumulateWithin(String accumulateWithin, int threshold, List<String> groupByAttributes) {
        List<GroupByAttribute> sanitizedAttributes = groupByAttributes.stream()
                .map(OnceAbstractTimeConstraint::sanitizeAttributeName)
                .map(GroupByAttribute::from)
                .toList();

        return new AccumulateWithinDefinition(TimeAmount.parseTimeAmount(accumulateWithin), threshold, sanitizedAttributes);
    }
}