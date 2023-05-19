/*
 * Copyright 2023 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.assertj.core.api.Assertions.assertThat;

public class MultipleRuleMatchTest {

    private String getJson(boolean matchMultipleRules) {
        String additionalAttribute = ""; // default is false
        if (matchMultipleRules) {
            additionalAttribute = "   \"match_multiple_rules\":true,\n";
        }

        String json =
                "{\n" +
                additionalAttribute +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"name\":\"R1\",\n" +
                "            \"condition\":{\n" +
                "               \"EqualsExpression\":{\n" +
                "                  \"lhs\":{\n" +
                "                     \"sensu\":\"data.i\"\n" +
                "                  },\n" +
                "                  \"rhs\":{\n" +
                "                     \"Integer\":1\n" +
                "                  }\n" +
                "               }\n" +
                "            }\n" +
                "         }\n" +
                "      },\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"name\":\"R2\",\n" +
                "            \"condition\":{\n" +
                "               \"EqualsExpression\":{\n" +
                "                  \"lhs\":{\n" +
                "                     \"sensu\":\"data.i\"\n" +
                "                  },\n" +
                "                  \"rhs\":{\n" +
                "                     \"Integer\":1\n" +
                "                  }\n" +
                "               }\n" +
                "            }\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        return json;
    }

    @Test
    public void executeFacts_shouldMatchMultipleRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(getJson(false));
        int executedRules = rulesExecutor.executeFacts("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(executedRules).isEqualTo(2);

        rulesExecutor.dispose();
    }

    @Test
    public void processFacts_shouldMatchMultipleRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(getJson(false));
        List<Match> matchedRules = rulesExecutor.processFacts("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(matchedRules).hasSize(2);
        assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R1", "R2");

        rulesExecutor.dispose();
    }

    @Test
    public void processEvents_shouldMatchOneRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(getJson(false));
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(matchedRules).hasSize(1);
        assertThat(matchedRules.get(0).getRule().getName()).isIn("R1", "R2");

        rulesExecutor.dispose();
    }

    @Test
    public void executeFactsWithMatchMultipleRules_shouldMatchMultipleRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(getJson(true));
        int executedRules = rulesExecutor.executeFacts("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(executedRules).isEqualTo(2);

        rulesExecutor.dispose();
    }

    @Test
    public void processFactsWithMatchMultipleRules_shouldMatchMultipleRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(getJson(true));
        List<Match> matchedRules = rulesExecutor.processFacts("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(matchedRules).hasSize(2);
        assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R1", "R2");

        rulesExecutor.dispose();
    }

    @Test
    public void processEventsWithMatchMultipleRules_shouldMatchMultipleRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(getJson(true));
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(matchedRules).hasSize(2);
        assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R1", "R2");

        rulesExecutor.dispose();
    }
}
