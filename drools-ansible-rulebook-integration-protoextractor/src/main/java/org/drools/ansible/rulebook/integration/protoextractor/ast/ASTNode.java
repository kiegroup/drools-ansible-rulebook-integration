package org.drools.ansible.rulebook.integration.protoextractor.ast;

public interface ASTNode {
    int getStartChar();

    int getEndChar();

    int getStartLine();

    int getStartColumn();

    int getEndLine();

    int getEndColumn();

    String getText();

    <T> T accept(Visitor<T> v);
}
