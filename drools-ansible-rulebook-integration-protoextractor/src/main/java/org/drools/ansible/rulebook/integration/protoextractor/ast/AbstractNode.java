package org.drools.ansible.rulebook.integration.protoextractor.ast;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

public abstract class AbstractNode implements ASTNode {
    protected final ASTNode[] EMPTY_CHILDREN = new ASTNode[0];
    private int startChar;
    private int endChar;
    private int startLine;
    private int startColumn;
    private int endLine;
    private int endColumn;

    private String text;

    protected AbstractNode( AbstractNode usingCtxFrom ) {
        copyLocationAttributesFrom(usingCtxFrom);
    }

    public AbstractNode( ParserRuleContext ctx ) {
        // DO NOT keep the reference to `ParserRuleContext` to avoid unneeded retention of lexer structures.
        this.setStartChar( ctx.getStart().getStartIndex() );
        this.setStartLine( ctx.getStart().getLine() );
        this.setStartColumn( ctx.getStart().getCharPositionInLine() );
        this.setEndChar( ctx.getStop().getStopIndex() );
        this.setEndLine( ctx.getStop().getLine() );
        this.setEndColumn( ctx.getStop().getCharPositionInLine() + ctx.getStop().getText().length() );
        this.setText( getOriginalText( ctx ) );
    }

    private AbstractNode copyLocationAttributesFrom(AbstractNode from) {
        this.setStartChar(from.getStartChar());
        this.setStartLine(from.getStartLine());
        this.setStartColumn(from.getStartColumn());
        this.setEndChar(from.getEndChar());
        this.setEndLine(from.getEndLine());
        this.setEndColumn(from.getEndColumn());
        this.setText(from.getText());
        return this;
    }

    public static String getOriginalText(ParserRuleContext ctx) {
        int a = ctx.start.getStartIndex();
        int b = ctx.stop.getStopIndex();
        Interval interval = new Interval( a, b );
        return ctx.getStart().getInputStream().getText( interval );
    }

    @Override
    public int getStartChar() {
        return startChar;
    }

    public void setStartChar(int startChar) {
        this.startChar = startChar;
    }

    @Override
    public int getEndChar() {
        return endChar;
    }

    public void setEndChar(int endChar) {
        this.endChar = endChar;
    }

    @Override
    public int getStartLine() {
        return startLine;
    }

    public void setStartLine(int startLine) {
        this.startLine = startLine;
    }

    @Override
    public int getStartColumn() {
        return startColumn;
    }

    public void setStartColumn(int startColumn) {
        this.startColumn = startColumn;
    }

    @Override
    public int getEndLine() {
        return endLine;
    }

    public void setEndLine(int endLine) {
        this.endLine = endLine;
    }

    @Override
    public int getEndColumn() {
        return endColumn;
    }

    public void setEndColumn(int endColumn) {
        this.endColumn = endColumn;
    }

    @Override
    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()+"{" + text + "}";
    }
}