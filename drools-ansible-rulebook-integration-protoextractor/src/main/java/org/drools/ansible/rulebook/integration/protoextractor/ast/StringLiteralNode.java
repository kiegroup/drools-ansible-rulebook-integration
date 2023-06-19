package org.drools.ansible.rulebook.integration.protoextractor.ast;

import org.antlr.v4.runtime.ParserRuleContext;

public class StringLiteralNode extends AbstractNode implements TextValue {

    private final String value;

    public StringLiteralNode(ParserRuleContext ctx) {
        super(ctx);
        String originalText = getOriginalText(ctx);
        this.value = originalText.substring(1, originalText.length()-1);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "StringLiteralNode [value=" + value + "]";
    }

    @Override
    public <T> T accept(Visitor<T> v) {
        return v.visit(this);
    }
}
