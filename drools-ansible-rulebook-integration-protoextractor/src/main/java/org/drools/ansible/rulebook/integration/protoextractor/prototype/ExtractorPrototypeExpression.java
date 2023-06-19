package org.drools.ansible.rulebook.integration.protoextractor.prototype;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.drools.ansible.rulebook.integration.protoextractor.ExtractorUtils;
import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;

import org.drools.model.Prototype;
import org.drools.model.PrototypeExpression;
import org.drools.model.PrototypeFact;
import org.drools.model.functions.Function1;

public class ExtractorPrototypeExpression implements PrototypeExpression {
    private final ExtractorNode extractorNode;

    ExtractorPrototypeExpression(ExtractorNode extractorNode) {
        this.extractorNode = extractorNode;
    }

    @Override
    public Function1<PrototypeFact, Object> asFunction(Prototype prototype) {
        return pf -> {
            Map<String, Object> asMap = ((org.drools.base.facttemplates.Fact) pf).asMap();
            Object value = new ValueExtractionVisitor(asMap).visit(extractorNode);
            return value != null ? value : Prototype.UNDEFINED_VALUE;
        };
    }

    // TODO used for indexing, normalizing chunks.
    public String getFieldName() {
        return ExtractorUtils.getParts(extractorNode).stream().collect(Collectors.joining());
    }

    @Override
    public String toString() {
        return "ExtractorPrototypeExpression{" + extractorNode + "}";
    }

    @Override
    public Collection<String> getImpactedFields() {
        return Collections.singletonList(ExtractorUtils.getParts(extractorNode).get(0));
    }
}
