package org.drools.ansible.rulebook.integration.protoextractor.prototype;

import org.drools.ansible.rulebook.integration.protoextractor.ExtractorParser;
import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;
import org.drools.model.PrototypeExpression;
import org.drools.model.PrototypeVariable;

public class ExtractorPrototypeExpressionUtils {
    private ExtractorPrototypeExpressionUtils() {
        // only static methods.
    }
    
    public static ExtractorPrototypeExpression prototypeFieldExtractor(String expression) {
        ExtractorNode parse = ExtractorParser.parse(expression);
        return new ExtractorPrototypeExpression(parse);
    }

    /** 
     * old way of: `prototypeField(rightField.substring(dotPos+1))`
     * given: m_0.a.b.c
     * returns an extractor for: a.b.c
     */ 
    public static ExtractorPrototypeExpression prototypeFieldExtractorSkippingFirst(String expression) {
        ExtractorNode parse = ExtractorParser.parse(expression);
        ExtractorNode parseSkipFirst = parse.cloneSkipFirst();
        return new ExtractorPrototypeExpression(parseSkipFirst);
    }

    public static PrototypeExpression prototypeFieldExtractor(PrototypeVariable betaVariable, String expression) {
        ExtractorNode parse = ExtractorParser.parse(expression);
        return new CompleteExtractorPrototypeExpression(betaVariable, parse);
    }
}
