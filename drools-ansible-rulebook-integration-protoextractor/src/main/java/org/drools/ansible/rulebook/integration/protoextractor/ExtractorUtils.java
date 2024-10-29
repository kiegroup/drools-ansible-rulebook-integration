package org.drools.ansible.rulebook.integration.protoextractor;

import java.util.List;

import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;
import org.drools.ansible.rulebook.integration.protoextractor.prototype.NormalizedFieldRepresentationVisitor;
import org.drools.ansible.rulebook.integration.protoextractor.prototype.ValueCollectVisitor;

public class ExtractorUtils {
    private ExtractorUtils() {
        // only static methods.
    }
    
    public static List<String> getParts(ExtractorNode extractorNode) {
        return new NormalizedFieldRepresentationVisitor().visit(extractorNode);
    }

    public static Object getValueFrom(ExtractorNode extractorNode, Object readValue) {
        return new ValueCollectVisitor(readValue).visit(extractorNode);
    }
}
