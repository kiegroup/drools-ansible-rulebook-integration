package org.drools.ansible.rulebook.integration.protoextractor.prototype;

import java.util.ArrayList;
import java.util.List;

import org.drools.ansible.rulebook.integration.protoextractor.ast.ASTNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.IdentifierNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.IndexAccessorNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.SquaredAccessorNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.Visitor;

public class NormalizedFieldRepresentationVisitor implements Visitor<List<String>> {
    List<String> result = new ArrayList<>();

    @Override
    public List<String> visit(ASTNode n) {
        throw new UnsupportedOperationException("Unimplemented method 'visit'");
    }

    @Override
    public List<String> visit(IdentifierNode n) {
        result.add(n.getValue());
        return result;
    }

    @Override
    public List<String> visit(SquaredAccessorNode n) {
        result.add(n.getValue());
        return result;
    }

    @Override
    public List<String> visit(IndexAccessorNode n) {
        result.add("("+n.getValue()+")");
        return result;
    }

    @Override
    public List<String> visit(ExtractorNode n) {
        for (ASTNode chunk : n.getValues()) {
            chunk.accept(this);
        }
        return result;
    }
}
