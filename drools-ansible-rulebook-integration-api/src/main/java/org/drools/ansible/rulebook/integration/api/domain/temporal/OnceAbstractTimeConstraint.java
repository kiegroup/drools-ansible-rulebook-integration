package org.drools.ansible.rulebook.integration.api.domain.temporal;

import java.util.List;

import org.drools.model.Index;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeVariable;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.model.PrototypeDSL.protoPattern;
import static org.drools.model.PrototypeDSL.variable;
import static org.drools.model.PrototypeExpression.fixedValue;
import static org.drools.model.PrototypeExpression.prototypeField;

public abstract class OnceAbstractTimeConstraint implements TimeConstraint {

    protected final String ruleName;

    protected final TimeAmount timeAmount;
    protected final List<String> groupByAttributes;

    protected PrototypeDSL.PrototypePatternDef guardedPattern;

    public OnceAbstractTimeConstraint(String ruleName, TimeAmount timeAmount, List<String> groupByAttributes) {
        this.ruleName = ruleName;
        this.timeAmount = timeAmount;
        this.groupByAttributes = groupByAttributes;
    }

    protected PrototypeVariable getPatternVariable() {
        return (PrototypeVariable) guardedPattern.getFirstVariable();
    }

    protected PrototypeDSL.PrototypePatternDef createControlPattern() {
        PrototypeDSL.PrototypePatternDef controlPattern = protoPattern(variable(getPrototype(SYNTHETIC_PROTOTYPE_NAME)));
        for (String unique : groupByAttributes) {
            controlPattern.expr( prototypeField(unique), Index.ConstraintType.EQUAL, getPatternVariable(), prototypeField(unique) );
        }
        controlPattern.expr( prototypeField("drools_rule_name"), Index.ConstraintType.EQUAL, fixedValue(ruleName) );
        return controlPattern;
    }

    static String sanitizeAttributeName(String name) {
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
