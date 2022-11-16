package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.drools.core.facttemplates.Event;
import org.drools.model.Drools;
import org.drools.model.Index;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeFact;
import org.drools.model.PrototypeVariable;
import org.drools.model.Rule;
import org.drools.model.view.CombinedExprViewItem;
import org.drools.model.view.ViewItem;

import static java.util.stream.Collectors.toList;
import static org.drools.ansible.rulebook.integration.api.RulesExecutor.SYNTHETIC_RULE_TAG;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.TimeAmount.parseTimeAmount;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.model.DSL.not;
import static org.drools.model.DSL.on;
import static org.drools.model.PatternDSL.rule;
import static org.drools.model.PrototypeDSL.protoPattern;
import static org.drools.model.PrototypeDSL.variable;
import static org.drools.model.PrototypeExpression.prototypeField;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedEvent;

/**
 * Coalesce events within a time window: if the same event is sent multiple times only one of them in a given time window
 * can trigger rules activation. The uniqueness criteria for the event must be specified via an additional attribute.
 *
 *    e.g.:
 *      condition:
 *         all:
 *           - singleton << event.sensu.process.type == "alert"
 *         once_within: 10 minutes
 *         unique_attributes:
 *             - event.sensu.host
 *             - event.sensu.process.type
 *
 * It has been implemented by inserting a synthetic event, expiring after the time window, when the rule matches for the first time,
 * to prevent further matches within that window plus another synthetic rule matching both the expected event and the synthetic event
 * in order to remove immediately from the working memory the duplicated events arrived in the same time window.
 *
 * In other words the former example is translated in the following 2 rules:
 *
 * rule R when
 *   singleton : Event( sensu.process.type == "alert" )
 *   not( Control( sensu.host == singleton.sensu.host, sensu.process.type == singleton.sensu.process.type ) )
 * then
 *   Control control = new Control().withExpiration(10, TimeUnit.MINUTE);
 *   control.set("sensu.host", singleton.sensu.host);
 *   control.set("sensu.process.type", singleton.sensu.process.type);
 *   insert(control);
 * end
 *
 * rule Cleanup when
 *   singleton : Event( sensu.process.type == "alert" )
 *   Control( sensu.host == singleton.sensu.host, sensu.process.type == singleton.sensu.process.type )
 * then
 *   delete(singleton);
 * end
 */
public class OnceWithinDefinition implements TimeConstraint {

    private final TimeAmount timeAmount;
    private final List<String> uniqueAttributes;

    PrototypeDSL.PrototypePatternDef guardedPattern;

    public OnceWithinDefinition(TimeAmount timeAmount, List<String> uniqueAttributes) {
        this.timeAmount = timeAmount;
        this.uniqueAttributes = uniqueAttributes;
    }

    @Override
    public PrototypeVariable getTimeConstraintConsequenceVariable() {
        return (PrototypeVariable) guardedPattern.getFirstVariable();
    }

    @Override
    public ViewItem appendTimeConstraint(ViewItem pattern) {
        guardedPattern = (PrototypeDSL.PrototypePatternDef) pattern;
        return new CombinedExprViewItem( org.drools.model.Condition.Type.AND, new ViewItem[] { guardedPattern, not( createControlPattern() ) } );
    }

    private PrototypeDSL.PrototypePatternDef createControlPattern() {
        PrototypeDSL.PrototypePatternDef controlPattern = protoPattern(variable(getPrototype(SYNTHETIC_PROTOTYPE_NAME)));
        for (String unique : uniqueAttributes) {
            controlPattern.expr( prototypeField(unique), Index.ConstraintType.EQUAL, getTimeConstraintConsequenceVariable(), prototypeField(unique) );
        }
        return controlPattern;
    }

    @Override
    public BiConsumer<Drools, PrototypeFact> getTimeConstraintConsequence() {
        return (Drools drools, PrototypeFact fact) -> {
            Event controlEvent = createMapBasedEvent( getPrototype(SYNTHETIC_PROTOTYPE_NAME) )
                    .withExpiration(timeAmount.getAmount(), timeAmount.getTimeUnit());
            for (String unique : uniqueAttributes) {
                controlEvent.set(unique, fact.get(unique));
            }
            drools.insert(controlEvent);
        };
    }

    @Override
    public Rule getControlRule() {
        return rule( "cleanup_" + UUID.randomUUID() ).metadata(SYNTHETIC_RULE_TAG, true)
                .build( guardedPattern,
                        createControlPattern(),
                        on(getTimeConstraintConsequenceVariable()).execute((drools, fact) -> drools.delete(fact)) );
    }

    @Override
    public String toString() {
        return "OnceWithinDefinition{" + " " + timeAmount + ", uniqueAttributes=" + uniqueAttributes + " }";
    }

    static OnceWithinDefinition parseOnceWithin(String onceWithin, List<String> uniqueAttributes) {
        List<String> sanitizedAttributes = uniqueAttributes.stream().map(OnceWithinDefinition::sanitizeAttributeName).collect(toList());
        return new OnceWithinDefinition(parseTimeAmount(onceWithin), sanitizedAttributes);
    }

    private static String sanitizeAttributeName(String name) {
        if (name.startsWith("event.")) {
            return name.substring("event.".length());
        }
        if (name.startsWith("events.")) {
            return name.substring("events.".length());
        }
        if (name.startsWith("fact.")) {
            return name.substring("fact.".length());
        }
        if (name.startsWith("facts.")) {
            return name.substring("facts.".length());
        }
        return name;
    }


}
