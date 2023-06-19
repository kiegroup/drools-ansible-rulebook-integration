package org.drools.ansible.rulebook.integration.protoextractor.ast;

import java.util.Objects;

import org.antlr.v4.runtime.ParserRuleContext;

public class SquaredAccessorNode extends AbstractNode {
    private final TextValue value;

    public SquaredAccessorNode(ParserRuleContext ctx, TextValue value) {
        super(ctx);
        Objects.requireNonNull(value);
        this.value = value;
    }

    public String getValue() {
        return this.value.getValue();
    }

    @Override
    public String toString() {
        return "SquaredAccessorNode [value=" + value + "]";
    }

    @Override
    public <T> T accept(Visitor<T> v) {
        return v.visit(this);
    }
}
