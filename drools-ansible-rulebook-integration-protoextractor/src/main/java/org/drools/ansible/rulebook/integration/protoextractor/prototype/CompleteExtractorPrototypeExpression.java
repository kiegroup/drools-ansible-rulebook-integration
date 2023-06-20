package org.drools.ansible.rulebook.integration.protoextractor.prototype;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;
import org.drools.model.PrototypeExpression.EvaluableExpression;
import org.drools.model.PrototypeFact;
import org.drools.model.PrototypeVariable;

public class CompleteExtractorPrototypeExpression extends ExtractorPrototypeExpression implements EvaluableExpression {
    private final PrototypeVariable protoVar;

    public CompleteExtractorPrototypeExpression(PrototypeVariable protoVar, ExtractorNode extractorNode) {
        super(extractorNode);
        this.protoVar = protoVar;
    }

    public PrototypeVariable getProtoVar() {
        return protoVar;
    }

    @Override
    public boolean hasPrototypeVariable() {
        return true;
    }

    @Override
    public Collection<PrototypeVariable> getPrototypeVariables() {
        return Collections.singletonList(protoVar);
    }

    @Override
    public Object evaluate(Map<PrototypeVariable, PrototypeFact> factsMap) {
        PrototypeFact prototypeFact = factsMap.get(protoVar);
        return asFunction(null).apply(prototypeFact);
    }

    @Override
    public String toString() {
        return "CompleteExtractorPrototypeExpression{" + extractorNode + "}";
    }
}
