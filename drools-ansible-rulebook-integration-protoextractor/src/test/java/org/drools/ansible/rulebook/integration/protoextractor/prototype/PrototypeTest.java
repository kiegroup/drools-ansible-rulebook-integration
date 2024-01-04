package org.drools.ansible.rulebook.integration.protoextractor.prototype;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.protoextractor.ExtractorParser;
import org.junit.Test;
import org.kie.api.prototype.PrototypeFactInstance;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.kie.api.prototype.PrototypeBuilder.prototype;

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
        final Map<String, Object> readValue = new ObjectMapper().readValue(JSON, new TypeReference<>() {});
        final PrototypeFactInstance mapBasedFact = prototype("test").asFact().newInstance();
        readValue.forEach(mapBasedFact::set);
        assertThat(mapBasedFact.get("a"))
            .as("sanity check on manually built drools Fact")
            .isNotNull()
            .isInstanceOf(Map.class);

        Object valueExtracted = expr.asFunction(ExtractorPrototypeExpression.IGNORED).apply(mapBasedFact);
        assertThat(valueExtracted)
            .as("ExtractorPrototypeExpression used to extract value based on the path expression")
            .isEqualTo(47);
    }
}
