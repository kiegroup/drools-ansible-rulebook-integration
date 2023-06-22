package org.drools.ansible.rulebook.integration.protoextractor.ast;

import java.util.Objects;

import org.antlr.v4.runtime.ParserRuleContext;

public class IndexAccessorNode extends AbstractNode {
    private final IntegerLiteralNode value;

    public IndexAccessorNode(ParserRuleContext ctx, IntegerLiteralNode value) {
        super(ctx);
        Objects.requireNonNull(value);
        this.value = value;
    }

    public Integer getValue() {
        return this.value.getValue();
    }

    @Override
    public String toString() {
        return "IndexAccessorNode [value=" + value + "]";
    }

    @Override
    public <T> T accept(Visitor<T> v) {
        return v.visit(this);
    }
}
