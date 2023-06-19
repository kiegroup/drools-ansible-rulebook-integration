package org.drools.ansible.rulebook.integration.protoextractor;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ExtractorTest {

    @Test
    public void testBasic() throws Exception {
        final String expression = "a.b[\"c\"].d[\"e/asd\"]['f'].g.h";

        ExtractorNode extractor = ExtractorParser.parse(expression);
        List<String> parts = ExtractorUtils.getParts(extractor);
        assertThat(parts)
            .as("a series of string chunks which could be used for indexing")
            .containsExactly("a", "b", "c", "d", "e/asd", "f", "g", "h");

        final String JSON = Files.readString(Paths.get(ExtractorTest.class.getResource("/test1.json").toURI()));
        Map<?, ?> readValue = new ObjectMapper().readValue(JSON, Map.class);
        Object valueExtracted = ExtractorUtils.getValueFrom(extractor, readValue);
        assertThat(valueExtracted)
            .as("extractor can be used to extract value based on the path expression")
            .isEqualTo(47);
    }

    @Test
    public void testMixedBag() throws Exception {
        final String expression = "range[\"x\"][1][2].a[\"b\"]";

        ExtractorNode extractor = ExtractorParser.parse(expression);

        final String JSON = Files.readString(Paths.get(ExtractorTest.class.getResource("/mixedBag.json").toURI()));
        Map<?, ?> readValue = new ObjectMapper().readValue(JSON, Map.class);
        Object valueExtracted = ExtractorUtils.getValueFrom(extractor, readValue);
        assertThat(valueExtracted)
            .as("extractor can be used to extract value based on the path expression")
            .isEqualTo(47);
    }

    @Test
    public void testMixedBagReverse() throws Exception {
        final String expression = "range[\"x\"][-1][2].a[\"b\"]";

        ExtractorNode extractor = ExtractorParser.parse(expression);

        final String JSON = Files.readString(Paths.get(ExtractorTest.class.getResource("/mixedBag.json").toURI()));
        Map<?, ?> readValue = new ObjectMapper().readValue(JSON, Map.class);
        Object valueExtracted = ExtractorUtils.getValueFrom(extractor, readValue);
        assertThat(valueExtracted)
            .as("extractor can be used to extract value based on the path expression")
            .isEqualTo(47);
    }
}
