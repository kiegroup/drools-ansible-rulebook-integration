package org.drools.ansible.rulebook.integration.protoextractor.ast;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.antlr.v4.runtime.ParserRuleContext;

public class ExtractorNode extends AbstractNode {
    private final List<ASTNode> values;

    public ExtractorNode(ParserRuleContext ctx, List<ASTNode> values2) {
        super(ctx);
        Objects.requireNonNull(values2);
        this.values = Collections.unmodifiableList(values2);
    }

    public List<ASTNode> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "ExtractorNode [values=" + values + "]";
    }

    @Override
    public <T> T accept(Visitor<T> v) {
        return v.visit(this);
    }
}
