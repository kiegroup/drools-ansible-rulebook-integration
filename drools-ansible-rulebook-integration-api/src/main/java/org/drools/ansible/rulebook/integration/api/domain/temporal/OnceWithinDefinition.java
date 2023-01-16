package org.drools.ansible.rulebook.integration.api.domain.temporal;

import java.util.Collections;
import java.util.List;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.core.facttemplates.Event;
import org.drools.core.facttemplates.Fact;
import org.drools.model.Drools;
import org.drools.model.DroolsEntryPoint;
import org.drools.model.PrototypeDSL;
import org.drools.model.Rule;
import org.drools.model.Variable;
import org.drools.model.view.CombinedExprViewItem;
import org.drools.model.view.ViewItem;

import static java.util.stream.Collectors.toList;
import static org.drools.ansible.rulebook.integration.api.domain.temporal.TimeAmount.parseTimeAmount;
import static org.drools.ansible.rulebook.integration.api.rulesengine.RegisterOnlyAgendaFilter.SYNTHETIC_RULE_TAG;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.model.DSL.not;
import static org.drools.model.DSL.on;
import static org.drools.model.PatternDSL.rule;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedEvent;

/**
 * Coalesce events within a time window: if the same event is sent multiple times only one of them in a given time window
 * can trigger rules activation. The uniqueness criteria for the event must be specified via an additional attribute.
 *
 *    e.g.:
 *      condition:
 *         all:
 *           - singleton << event.sensu.process.type == "alert"
 *         throttle:
 *           once_within: 10 minutes
 *           group_by_attributes:
 *             - event.sensu.host
 *             - event.sensu.process.type
 *
 * It has been implemented by inserting a synthetic event, expiring after the time window, when the rule matches for the first time,
 * to prevent further matches within that window plus another synthetic rule matching both the expected event and the synthetic event
 * in order to remove immediately from the working memory the duplicated events arrived in the same time window.
 *
 * In other words the former example is translated into the following 2 rules:
 *
 * rule R when
 *   singleton : Event( sensu.process.type == "alert" )
 *   not( Control( sensu.host == singleton.sensu.host, sensu.process.type == singleton.sensu.process.type, drools_rule_name == "R" ) )
 * then
 *   Control control = new Control().withExpiration(10, TimeUnit.MINUTE);
 *   control.set("sensu.host", singleton.sensu.host);
 *   control.set("sensu.process.type", singleton.sensu.process.type);
 *   control.set("drools_rule_name", "R");
 *   insert(control);
 *   delete(singleton);
 * end
 *
 * rule Cleanup when
 *   singleton : Event( sensu.process.type == "alert" )
 *   Control( sensu.host == singleton.sensu.host, sensu.process.type == singleton.sensu.process.type, drools_rule_name == "R" )
 * then
 *   delete(singleton);
 * end
 */
public class OnceWithinDefinition extends OnceAbstractTimeConstraint {

    public static final String KEYWORD = "once_within";

    public OnceWithinDefinition(TimeAmount timeAmount, List<String> groupByAttributes) {
        super(timeAmount, groupByAttributes);
    }

    @Override
    public boolean requiresAsyncExecution() {
        return false;
    }

    @Override
    public Variable<?>[] getTimeConstraintConsequenceVariables() {
        return new Variable[] { getPatternVariable() };
    }

    @Override
    public void executeTimeConstraintConsequence(Drools drools, Object... facts) {
        Event controlEvent = createMapBasedEvent( getPrototype(SYNTHETIC_PROTOTYPE_NAME) )
                .withExpiration(timeAmount.getAmount(), timeAmount.getTimeUnit());
        Fact fact = (Fact) facts[0];
        for (String unique : groupByAttributes) {
            controlEvent.set(unique, fact.get(unique));
        }
        controlEvent.set("drools_rule_name", ruleName);
        drools.insert(controlEvent);
        drools.delete(fact);
    }

    @Override
    public ViewItem processTimeConstraint(String ruleName, ViewItem pattern) {
        this.ruleName = ruleName;
        if (guardedPattern != null) {
            throw new IllegalStateException("Cannot process this TimeConstraint twice");
        }
        guardedPattern = (PrototypeDSL.PrototypePatternDef) pattern;
        return new CombinedExprViewItem( org.drools.model.Condition.Type.AND, new ViewItem[] { guardedPattern, not( createControlPattern() ) } );
    }

    @Override
    public List<Rule> getControlRules(RuleGenerationContext ruleContext) {
        return Collections.singletonList(
                rule( "cleanup_" + ruleName ).metadata(SYNTHETIC_RULE_TAG, true)
                        .build( guardedPattern,
                                createControlPattern(),
                                on(getPatternVariable()).execute(DroolsEntryPoint::delete) )
        );
    }

    @Override
    public String toString() {
        return "OnceWithinDefinition{" + " " + timeAmount + ", groupByAttributes=" + groupByAttributes + " }";
    }

    public static OnceWithinDefinition parseOnceWithin(String onceWithin, List<String> groupByAttributes) {
        List<String> sanitizedAttributes = groupByAttributes.stream().map(OnceAbstractTimeConstraint::sanitizeAttributeName).collect(toList());
        return new OnceWithinDefinition(parseTimeAmount(onceWithin), sanitizedAttributes);
    }
}
