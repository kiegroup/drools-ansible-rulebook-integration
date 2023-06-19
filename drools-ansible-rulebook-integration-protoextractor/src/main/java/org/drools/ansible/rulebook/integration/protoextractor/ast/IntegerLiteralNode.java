package org.drools.ansible.rulebook.integration.protoextractor.ast;

import org.antlr.v4.runtime.ParserRuleContext;

public class IntegerLiteralNode extends AbstractNode {

    private final Integer value;

    public IntegerLiteralNode(ParserRuleContext ctx) {
        super(ctx);
        String originalText = getOriginalText(ctx);
        this.value = Integer.valueOf(originalText);
    }

    public Integer getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "IntegerLiteralNode [value=" + value + "]";
    }

    @Override
    public <T> T accept(Visitor<T> v) {
        return v.visit(this);
    }
}
