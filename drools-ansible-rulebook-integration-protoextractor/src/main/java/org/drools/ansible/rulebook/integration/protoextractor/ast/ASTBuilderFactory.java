package org.drools.ansible.rulebook.integration.protoextractor.ast;

import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;

public class ASTBuilderFactory {
    private ASTBuilderFactory() {
        // only static utils methods.
    }
    public static StringLiteralNode newStringLiteralNode(ParserRuleContext ctx) {
        return new StringLiteralNode( ctx );
    }
    public static IntegerLiteralNode newIntegerLiteralNode(ParserRuleContext ctx) {
        return new IntegerLiteralNode( ctx );
    }
    public static IdentifierNode newIdentifierNode(ParserRuleContext ctx) {
        return new IdentifierNode( ctx );
    }
    public static SquaredAccessorNode newSquaredAccessorNode(ParserRuleContext ctx, TextValue value) {
        return new SquaredAccessorNode( ctx, value );
    }
    public static IndexAccessorNode newIndexAccessorNode(ParserRuleContext ctx, IntegerLiteralNode value) {
        return new IndexAccessorNode( ctx, value );
    }
    public static ExtractorNode newExtractorNode(ParserRuleContext ctx, List<ASTNode> values) {
        return new ExtractorNode( ctx, values );
    }
}
