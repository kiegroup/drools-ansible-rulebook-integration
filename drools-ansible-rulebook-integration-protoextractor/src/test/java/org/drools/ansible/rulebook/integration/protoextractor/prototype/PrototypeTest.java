package org.drools.ansible.rulebook.integration.protoextractor.prototype;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.protoextractor.ExtractorParser;
import org.drools.base.facttemplates.Fact;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeFact;
import org.drools.modelcompiler.facttemplate.FactFactory;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PrototypeTest {

    @Test
    public void testBasic() throws Exception {
        final String expression = "a.b[\"c\"].d[\"e/asd\"]['f'].g.h";

        ExtractorPrototypeExpression expr = new ExtractorPrototypeExpression(ExtractorParser.parse(expression));
        assertThat(expr.getImpactedFields())
            .as("always the first of the chunks")
            .isEqualTo(List.of("a"));
        assertThat(expr.getIndexingKey())
            .as("TODO normalization of chunks used for indexing")
            .isPresent()
            .contains("abcde/asdfgh");

        final String JSON = Files.readString(Paths.get(PrototypeTest.class.getResource("/test1.json").toURI()));
        final Map<String, Object> readValue = new ObjectMapper().readValue(JSON, new TypeReference<Map<String, Object>>() {});
        final Fact mapBasedFact = FactFactory.createMapBasedFact(PrototypeDSL.prototype("test"), readValue);
        assertThat(mapBasedFact.get("a"))
            .as("sanity check on manually built drools Fact")
            .isNotNull()
            .isInstanceOf(Map.class);

        Object valueExtracted = expr.asFunction(ExtractorPrototypeExpression.IGNORED).apply((PrototypeFact) mapBasedFact);
        assertThat(valueExtracted)
            .as("ExtractorPrototypeExpression used to extract value based on the path expression")
            .isEqualTo(47);
    }
}
