package org.drools.ansible.rulebook.integration.api.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.io.JsonMapper;

import org.junit.Test;

public class JSONParsingWithJacksonTest {

    @Test
    public void testAccessArrayField() {
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
