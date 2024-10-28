package org.drools.ansible.rulebook.integration.protoextractor.prototype;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.protoextractor.ast.ASTNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.DefaultedVisitor;
import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.IdentifierNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.IndexAccessorNode;
import org.drools.ansible.rulebook.integration.protoextractor.ast.SquaredAccessorNode;
import org.kie.api.prototype.Prototype;

public class ValueCollectVisitor extends DefaultedVisitor<Object> {
    private final Object original;
    private Object cur; // cur doesn't have to one Node in the event, it can be List of Nodes matching the path

    public ValueCollectVisitor(Object original) {
        this.original = original;
        this.cur = this.original;
    }

    @Override
    public Object defaultVisit(ASTNode n) {
        throw new UnsupportedOperationException("this visitor implemented all visit methods");
    }

    @Override
    public Object visit(IdentifierNode n) {
        if (cur instanceof List) {
            cur = collectPathsForAllElementsInArray(n);
        } else {
            cur = fromMap(cur, n.getValue());
        }
        return cur;
    }

    private Object collectPathsForAllElementsInArray(IdentifierNode n) {
        // if accessing a list without an index, evaluate all elements and collect matching children
        List<?> theList = (List<?>) cur;
        List<Object> nextNodeList = new PathWrapperList();
        for (Object element : theList) {
            Object nextNode = fromMap(element, n.getValue());
            if (nextNode != Prototype.UNDEFINED_VALUE) {
                nextNodeList.add(nextNode);
            }
        }
        if (nextNodeList.isEmpty()) {
            return Prototype.UNDEFINED_VALUE;
        } else {
            return nextNodeList;
        }
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
            if (javaIdx < theList.size()) { // avoid index out of bounds
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
            if (this.cur instanceof PathWrapperList currentNodeList) {
                PathWrapperList nextNodeList = new PathWrapperList();
                // Apply extraction to all paths in the list
                for (Object element : currentNodeList) {
                    this.cur = element;
                    Object nextNode = chunk.accept(this);
                    if (nextNode != Prototype.UNDEFINED_VALUE) {
                        nextNodeList.add(nextNode);
                    }
                }
                if (nextNodeList.isEmpty()) {
                    this.cur = Prototype.UNDEFINED_VALUE;
                } else {
                    // Flatten PathWrapperList if nested
                    nextNodeList = flattenPathWrapperList(nextNodeList);
                    this.cur = nextNodeList;
                }
            } else {
                this.cur = chunk.accept(this);
            }
        }

        // At this point, cur is wrapped in one PathWrapperList at most
        cur = stripPathWrapperListIfExists(cur);
        return cur;
    }

    private PathWrapperList flattenPathWrapperList(PathWrapperList pathWrapperList) {
        PathWrapperList flattenedList = new PathWrapperList();
        for (Object element : pathWrapperList) {
            if (element instanceof PathWrapperList nestedPathWrapperList) {
                flattenedList.addAll(nestedPathWrapperList); // nest is at most one level deep
            } else {
                flattenedList.add(element);
            }
        }
        return flattenedList;
    }

    private static Object stripPathWrapperListIfExists(Object current) {
        if (current instanceof PathWrapperList pathWrapperList) {
            if (pathWrapperList.isEmpty()) {
                return Prototype.UNDEFINED_VALUE;
            }
            // if the elements are lists, flatten them
            // if not, collect them in a list
            return pathWrapperList.stream()
                    .flatMap(e -> e instanceof List list ? list.stream() : List.of(e).stream())
                    .toList();
        } else {
            return current;
        }
    }

    // This List is used to wrap the node paths when the path indicates all elements of an array
    // Defined this class to differentiate from ArrayList which is used for an event array
    public static class PathWrapperList extends ArrayList<Object> {
    }
}
