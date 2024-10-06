package org.drools.ansible.rulebook.integration.protoextractor.prototype;

import org.drools.ansible.rulebook.integration.protoextractor.ExtractorUtils;
import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;
import org.drools.model.functions.Function1;
import org.drools.model.prototype.PrototypeDSL;
import org.drools.model.prototype.PrototypeExpression;
import org.kie.api.prototype.Prototype;
import org.kie.api.prototype.PrototypeFact;
import org.kie.api.prototype.PrototypeFactInstance;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ExtractorPrototypeExpression implements PrototypeExpression {
    /**
     * This is okay for ansible-integration work as prototype do not define accessor programmatically,
     * but having prototype definition ignored in {@link #asFunction(Prototype)} call, ought to be revised 
     * if porting this module into Drools and generalizing beyond usage in ansible-integration.
     */
    public static final PrototypeFact IGNORED = PrototypeDSL.prototypeFact("IGNORED");
    protected final ExtractorNode extractorNode;
    protected final String computedFieldName;
    protected final Collection<String> computedImpactedFields;

    public ExtractorPrototypeExpression(ExtractorNode extractorNode) {
        this.extractorNode = extractorNode;
        this.computedFieldName = ExtractorUtils.getParts(extractorNode).stream().collect(Collectors.joining());
        this.computedImpactedFields = Collections.singletonList(ExtractorUtils.getParts(extractorNode).get(0));
    }

    @Override
    public Function1<PrototypeFactInstance, Object> asFunction(Prototype prototype) {
        return pf -> {
            Map<String, Object> asMap = pf.asMap();
            Object value = ExtractorUtils.getValueFrom(extractorNode, asMap);
            return value;
        };
    }

    @Override
    public Optional<String> getIndexingKey() {
        return Optional.of(this.computedFieldName);
    }

    @Override
    public String toString() {
        return "ExtractorPrototypeExpression{" + extractorNode + "}";
    }

    @Override
    public Collection<String> getImpactedFields() {
        return this.computedImpactedFields;
    }
}
