package org.drools.ansible.rulebook.integration.protoextractor;

import java.util.ArrayList;
import java.util.List;

import org.drools.ansible.rulebook.integration.protoextractor.ProtoextractorParser.ChunkContext;
import org.drools.ansible.rulebook.integration.protoextractor.ProtoextractorParser.ExtractorContext;
import org.drools.ansible.rulebook.integration.protoextractor.ProtoextractorParser.IdentifierContext;
import org.drools.ansible.rulebook.integration.protoextractor.ProtoextractorParser.IndexAccessorContext;
import org.drools.ansible.rulebook.integration.protoextractor.ProtoextractorParser.IntegerLiteralContext;
import org.drools.ansible.rulebook.integration.protoextractor.ProtoextractorParser.SquaredAccessorContext;
import org.drools.ansible.rulebook.integration.protoextractor.ProtoextractorParser.StringLiteralContext;
import org.drools.ansible.rulebook.integration.protoextractor.ast.ASTBuilderFactory;
import org.drools.ansible.rulebook.integration.protoextractor.ast.ASTNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.IntegerLiteralNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.TextValue;

public class ASTProductionVisitor extends ProtoextractorBaseVisitor<ASTNode> {

    @Override
    public ASTNode visitStringLiteral(StringLiteralContext ctx) {
        return ASTBuilderFactory.newStringLiteralNode(ctx);
    }

    @Override
    public ASTNode visitIdentifier(IdentifierContext ctx) {
        return ASTBuilderFactory.newIdentifierNode(ctx);
    }

    @Override
    public ASTNode visitIntegerLiteral(IntegerLiteralContext ctx) {
        return ASTBuilderFactory.newIntegerLiteralNode(ctx);
    }

    @Override
    public ASTNode visitSquaredAccessor(SquaredAccessorContext ctx) {
        TextValue str = (TextValue) ctx.stringLiteral().accept(this);
        return ASTBuilderFactory.newSquaredAccessorNode(ctx, str);
    }

    @Override
    public ASTNode visitIndexAccessor(IndexAccessorContext ctx) {
        IntegerLiteralNode idx = (IntegerLiteralNode) ctx.integerLiteral().accept(this);
        return ASTBuilderFactory.newIndexAccessorNode(ctx, idx);
    }

    @Override
    public ASTNode visitChunk(ChunkContext ctx) {
        if (ctx.identifier() != null) {
            return ctx.identifier().accept(this);
        } else if (ctx.squaredAccessor() != null) {
            return ctx.squaredAccessor().accept(this);
        } else if (ctx.indexAccessor() != null) {
            return ctx.indexAccessor().accept(this);
        }
        throw new IllegalStateException("reached illegal state while parsing");
    }

    @Override
    public ASTNode visitExtractor(ExtractorContext ctx) {
        List<ASTNode> values = new ArrayList<>();
        ASTNode value0 = ctx.identifier().accept(this);
        values.add(value0);
        for (ChunkContext chunk : ctx.chunk()) {
            values.add(chunk.accept(this));
        }
        return ASTBuilderFactory.newExtractorNode(ctx, values);
    }
}
