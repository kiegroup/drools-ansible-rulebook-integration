package org.drools.ansible.rulebook.integration.protoextractor.prototype;

import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;
import org.drools.model.prototype.PrototypeExpression.EvaluableExpression;
import org.drools.model.prototype.PrototypeVariable;
import org.kie.api.prototype.PrototypeFactInstance;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

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
    public Object evaluate(Map<PrototypeVariable, PrototypeFactInstance> factsMap) {
        PrototypeFactInstance prototypeFact = factsMap.get(protoVar);
        return asFunction(IGNORED).apply(prototypeFact);
    }

    @Override
    public String toString() {
        return "CompleteExtractorPrototypeExpression{" + extractorNode + "}";
    }
}
