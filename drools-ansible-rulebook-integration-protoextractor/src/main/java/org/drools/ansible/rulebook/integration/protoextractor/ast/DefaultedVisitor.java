package org.drools.ansible.rulebook.integration.protoextractor.ast;

public abstract class DefaultedVisitor<T> implements Visitor<T> {
    public abstract T defaultVisit(ASTNode n);
    public T visit(ASTNode n) {
        return defaultVisit(n);
    }
    public T visit(IdentifierNode n) {
        return defaultVisit(n);
    }
    public T visit(SquaredAccessorNode n) {
        return defaultVisit(n);
    }
    public T visit(IndexAccessorNode n) {
        return defaultVisit(n);
    }
    public T visit(ExtractorNode n) {
        return defaultVisit(n);
    }
}
