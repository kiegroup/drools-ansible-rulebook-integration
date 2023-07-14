package org.drools.ansible.rulebook.integration.api.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.json.JSONObject;
import org.junit.Test;

public class JSONParsingWithOrgJsonJsonTest {

    @Test
    public void testAccessArrayField() {
        assertThat(new JSONObject("{\"results\": [1,2,3]}").getJSONArray("results").toList())
            .isNotNull()
            .isInstanceOf(List.class);

        assertThatThrownBy(() -> new JSONObject("{\"results\": [1,2,3]}").getJSONArray("unexistent key"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("unexistent key");

        assertThatThrownBy(() -> new JSONObject("{\"results\": 47}").getJSONArray("results"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("JSONObject[\"results\"] is not a JSONArray");
    }
}
