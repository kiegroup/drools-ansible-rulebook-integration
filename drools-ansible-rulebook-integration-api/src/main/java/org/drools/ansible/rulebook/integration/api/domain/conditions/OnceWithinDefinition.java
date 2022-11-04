package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.drools.ansible.rulebook.integration.api.RuleGenerationContext;
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
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.model.DSL.not;
import static org.drools.model.DSL.on;
import static org.drools.model.PatternDSL.rule;
import static org.drools.model.PrototypeDSL.protoPattern;
import static org.drools.model.PrototypeDSL.variable;
import static org.drools.model.PrototypeExpression.prototypeField;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedEvent;

public class OnceWithinDefinition {

    private final int amount;
    private final TimeUnit timeUnit;
    private final List<String> uniqueAttributes;

    PrototypeDSL.PrototypePatternDef guardedPattern;

    public OnceWithinDefinition(int amount, TimeUnit timeUnit, List<String> uniqueAttributes) {
        this.amount = amount;
        this.timeUnit = timeUnit;
        this.uniqueAttributes = uniqueAttributes;
    }

    public PrototypeVariable getGuardedVariable() {
        return (PrototypeVariable) guardedPattern.getFirstVariable();
    }

    public ViewItem appendGuardPattern(RuleGenerationContext ruleContext, ViewItem pattern) {
        guardedPattern = (PrototypeDSL.PrototypePatternDef) pattern;
        return new CombinedExprViewItem( org.drools.model.Condition.Type.AND, new ViewItem[] { guardedPattern, not( createControlPattern(ruleContext) ) } );
    }

    private PrototypeDSL.PrototypePatternDef createControlPattern(RuleGenerationContext ruleContext) {
        PrototypeDSL.PrototypePatternDef controlPattern = protoPattern(variable(getPrototype(SYNTHETIC_PROTOTYPE_NAME)));
        for (String unique : uniqueAttributes) {
            controlPattern.expr( prototypeField(unique), Index.ConstraintType.EQUAL, getGuardedVariable(), prototypeField(unique) );
        }
        return controlPattern;
    }

    public BiConsumer<Drools, PrototypeFact> insertGuardConsequence(RuleGenerationContext ruleContext) {
        return (Drools drools, PrototypeFact fact) -> {
            Event controlEvent = createMapBasedEvent( getPrototype(SYNTHETIC_PROTOTYPE_NAME) )
                    .withExpiration(amount, timeUnit);
            for (String unique : uniqueAttributes) {
                controlEvent.set(unique, fact.get(unique));
            }
            drools.insert(controlEvent);
        };
    }

    public Rule cleanupRule(RuleGenerationContext ruleContext) {
        return rule( "cleanup_" + UUID.randomUUID() ).metadata(SYNTHETIC_RULE_TAG, true)
                .build( guardedPattern,
                        createControlPattern(ruleContext),
                        on(getGuardedVariable()).execute((drools, fact) -> drools.delete(fact)) );
    }

    @Override
    public String toString() {
        return "OnceWithinDefinition{" +
                "value=" + amount +
                ", timeUnit=" + timeUnit +
                ", uniqueAttributes=" + uniqueAttributes +
                '}';
    }

    static OnceWithinDefinition parseOnceWithin(String onceWithin, List<String> uniqueAttributes) {
        int sepPos = onceWithin.indexOf(' ');
        if (sepPos <= 0) {
            throw new IllegalArgumentException("Invalid once_within definition: " + onceWithin);
        }
        int value = Integer.parseInt(onceWithin.substring(0, sepPos).trim());
        TimeUnit timeUnit = parseTimeUnit(onceWithin.substring(sepPos+1).trim());
        List<String> sanitizedAttributes = uniqueAttributes.stream().map(OnceWithinDefinition::sanitizeAttributeName).collect(toList());
        return new OnceWithinDefinition(value, timeUnit, sanitizedAttributes);
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

    private static TimeUnit parseTimeUnit(String unit) {
        if (unit.equalsIgnoreCase("millisecond") || unit.equalsIgnoreCase("milliseconds")) {
            return TimeUnit.MILLISECONDS;
        }
        if (unit.equalsIgnoreCase("second") || unit.equalsIgnoreCase("seconds")) {
            return TimeUnit.SECONDS;
        }
        if (unit.equalsIgnoreCase("minute") || unit.equalsIgnoreCase("minutes")) {
            return TimeUnit.MINUTES;
        }
        if (unit.equalsIgnoreCase("hour") || unit.equalsIgnoreCase("hours")) {
            return TimeUnit.HOURS;
        }
        if (unit.equalsIgnoreCase("day") || unit.equalsIgnoreCase("days")) {
            return TimeUnit.DAYS;
        }
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
}
