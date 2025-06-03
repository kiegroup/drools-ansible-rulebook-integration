package org.drools.ansible.rulebook.integration.api.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.io.JsonMapper;

import org.junit.jupiter.api.Test;

/*
 * The goal of this test was to demonstrate identical behaviour
 * between
 * - org.json:json 
 * - and Jackson (via JsonMapper util class of this project)
 * extracting a field in a json-object being an json-array.
 * Ref abff8e133da891569950b8252ed583b4b28b0829 in the PR.
 */
public class JSONParsingWithJacksonTest {

    @Test
    void testAccessArrayField() {
        assertThat(JsonMapper.readValueExtractFieldAsList("{\"results\": [1,2,3]}", "results"))
            .isNotNull()
            .isInstanceOf(List.class);

        assertThatThrownBy(() -> JsonMapper.readValueExtractFieldAsList("{\"results\": [1,2,3]}", "unexistent key"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("unexistent key");

        assertThatThrownBy(() -> JsonMapper.readValueExtractFieldAsList("{\"results\": 47}", "results"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Cannot deserialize");
    }
}
