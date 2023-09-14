package org.drools.ansible.rulebook.integration.protoextractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.protoextractor.ast.ExtractorNode;
import org.drools.model.Prototype;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
    public void testSquaredRoot() throws Exception {
        final String expression = "[\"a\"].b[\"c\"].d[\"e/asd\"]['f'].g.h";

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

    @Test
    public void testMixedBagNullRange2() throws Exception {
        assertThat(valueFromMixedBag("range2"))
            .as("the key `range2` exists as a first level entry in the json, but the value is the null literal") 
            .isNull();
    }

    @Test
    public void testMixedBagUndefFirstLevel() throws Exception {
        assertThat(valueFromMixedBag("unexisting") == Prototype.UNDEFINED_VALUE)
            .as("this key does not exists at all in the json")
            .isTrue();
    }

    @Test
    public void testMixedBagArrayIndexExistsButNullValue() throws Exception {
        assertThat(valueFromMixedBag("range.x[0]"))
            .as("this is accessing index 0 so the first element in a json array of size 2, but the first element in the json array is the null literal")
            .isNull();
    }

        @Test
    public void testMixedBagArrayIndexUnexisting() throws Exception {
        assertThat(valueFromMixedBag("range.x[999]") == Prototype.UNDEFINED_VALUE)
            .as("the json array is of size 2, so this 999 index position is undef")
            .isTrue();
    }

    private Object valueFromMixedBag(String expression) throws Exception {
        ExtractorNode extractor = ExtractorParser.parse(expression);

        final String JSON = Files.readString(Paths.get(ExtractorTest.class.getResource("/mixedBag.json").toURI()));
        Map<?, ?> readValue = new ObjectMapper().readValue(JSON, Map.class);
        Object valueExtracted = ExtractorUtils.getValueFrom(extractor, readValue);
        return valueExtracted;
    }
}
