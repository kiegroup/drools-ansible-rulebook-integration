package org.drools.ansible.rulebook.integration.api.domain.temporal;

import org.drools.ansible.rulebook.integration.protoextractor.ExtractorParser;
import org.drools.ansible.rulebook.integration.protoextractor.ExtractorUtils;
import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;
import org.drools.ansible.rulebook.integration.protoextractor.prototype.ExtractorPrototypeExpression;
import org.drools.ansible.rulebook.integration.protoextractor.prototype.ExtractorPrototypeExpressionUtils;
import org.drools.base.facttemplates.Fact;
import org.drools.model.Index;
import org.drools.model.prototype.PrototypeDSL;
import org.drools.model.prototype.PrototypeExpression;
import org.drools.model.prototype.PrototypeFact;
import org.drools.model.prototype.PrototypeVariable;

import java.util.List;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.model.prototype.PrototypeDSL.protoPattern;
import static org.drools.model.prototype.PrototypeDSL.variable;
import static org.drools.model.prototype.PrototypeExpression.fixedValue;
import static org.drools.model.prototype.PrototypeExpression.prototypeField;

public abstract class OnceAbstractTimeConstraint implements TimeConstraint {

    protected String ruleName;

    protected final TimeAmount timeAmount;
    protected final List<GroupByAttribute> groupByAttributes;

    protected PrototypeDSL.PrototypePatternDef guardedPattern;

    private PrototypeVariable controlVariable;

    public OnceAbstractTimeConstraint(TimeAmount timeAmount, List<GroupByAttribute> groupByAttributes) {
        this.timeAmount = timeAmount;
        this.groupByAttributes = groupByAttributes;
    }

    protected PrototypeVariable getPatternVariable() {
        return (PrototypeVariable) guardedPattern.getFirstVariable();
    }

    protected PrototypeVariable getControlVariable() {
        return controlVariable;
    }

    protected PrototypeDSL.PrototypePatternDef createControlPattern() {
        PrototypeDSL.PrototypePatternDef controlPattern = protoPattern(variable(getPrototype(SYNTHETIC_PROTOTYPE_NAME)));
        for (GroupByAttribute unique : groupByAttributes) {
            controlPattern.expr( prototypeField(unique.getKey()), // intentional, the control fact has the "group by" key string as-is (not structured), so we reference it for the left part
                    Index.ConstraintType.EQUAL,
                    getPatternVariable(),
                    unique.asPrototypeExpression() ); // on the right, we need extractor to check the real fact/event attribute value
        }
        controlPattern.expr( "drools_rule_name", Index.ConstraintType.EQUAL, ruleName );
        this.controlVariable = (PrototypeVariable) controlPattern.getFirstVariable();
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
    
    public static class GroupByAttribute {
        private final String key;
        private final ExtractorNode extractor;

        private GroupByAttribute(String key, ExtractorNode extractor) {
            this.key = key;
            this.extractor = extractor;
        }

        public static GroupByAttribute from(String expr) {
            return new GroupByAttribute(expr, ExtractorParser.parse(expr));
        }
        
        public String getKey() {
            return key;
        }

        public PrototypeExpression asPrototypeExpression() {
            return new ExtractorPrototypeExpression(extractor);
        }

        public Object evalExtractorOnFact(PrototypeFact fact) {
            return evalExtractorOnFact((Fact) fact);
        }

        public Object evalExtractorOnFact(Fact fact) {
            return ExtractorUtils.getValueFrom(extractor, fact.asMap());
        }

        @Override
        public String toString() {
            return "GroupByAttribute [key=" + key + ", extractor=" + extractor + "]";
        }
    }
}
