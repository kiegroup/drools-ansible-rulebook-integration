package org.drools.ansible.rulebook.integration.protoextractor.prototype;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.protoextractor.ast.ASTNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.DefaultedVisitor;
import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.IdentifierNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.IndexAccessorNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.SquaredAccessorNode;
import org.drools.model.Prototype;

public class ValueExtractionVisitor extends DefaultedVisitor<Object> {
    private final Object original;
    private Object cur;

    public ValueExtractionVisitor(Object original) {
        this.original = original;
        this.cur = this.original;
    }

    @Override
    public Object defaultVisit(ASTNode n) {
        cur = null;
        return cur;
    }

    @Override
    public Object visit(IdentifierNode n) {
        cur = fromMap(cur, n.getValue());
        return cur;
    }

    private static Object fromMap(Object in, String key) {
        if (in instanceof Map) {
            Map<?, ?> theMap = (Map<?, ?>) in;
            if (theMap.containsKey(key)) {
                return theMap.get(key);
            } else {
                return Prototype.UNDEFINED_VALUE;
            }
        } else {
            return Prototype.UNDEFINED_VALUE;
        }
    }

    @Override
    public Object visit(SquaredAccessorNode n) {
        cur = fromMap(cur, n.getValue());
        return cur;
    }

    @Override
    public Object visit(IndexAccessorNode n) {
        if (cur instanceof List) {
            List<?> theList = (List<?>) cur;
            int javaIdx = n.getValue() >= 0 ? n.getValue() : theList.size() + n.getValue();
            if (javaIdx < theList.size()) { // avoid index out of bounds;
                cur = theList.get(javaIdx); 
            } else {
                cur = Prototype.UNDEFINED_VALUE;
            }
        } else {
            cur = Prototype.UNDEFINED_VALUE;
        }
        return cur;
    }

    @Override
    public Object visit(ExtractorNode n) {
        for (ASTNode chunk : n.getValues()) {
            if (this.cur == null || this.cur == Prototype.UNDEFINED_VALUE) {
                break;
            }
            this.cur = chunk.accept(this);
        }
        return cur;
    }
}
