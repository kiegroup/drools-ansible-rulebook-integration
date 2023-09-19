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

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;

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

    @Test
    public void retainLeftPartialMatchesWithMatchMultipleRules() {
        checkPartialMatchesWithMatchMultipleRules(true, true);
    }

    @Test
    public void discardLeftPartialMatchesWithMatchMultipleRules() {
        checkPartialMatchesWithMatchMultipleRules(false, true);
    }

    @Test
    public void retainRightPartialMatchesWithMatchMultipleRules() {
        checkPartialMatchesWithMatchMultipleRules(true, false);
    }

    @Test
    public void discardRightPartialMatchesWithMatchMultipleRules() {
        checkPartialMatchesWithMatchMultipleRules(false, false);
    }

    private static void checkPartialMatchesWithMatchMultipleRules(boolean matchMultipleRules, boolean partialOnLeft) {
        String rules =
                "{\n" +
                "   \"match_multiple_rules\":" + matchMultipleRules + ",\n" +
                "   \"rules\": [\n" +
                "                {\n" +
                "                    \"Rule\": {\n" +
                "                        \"name\": \"R1\",\n" +
                "                        \"condition\": {\n" +
                "                            \"AllCondition\": [\n" +
                "                                {\n" +
                "                                    \"EqualsExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"i\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"Integer\": 0\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            ]\n" +
                "                        },\n" +
                "                        \"actions\": [\n" +
                "                            {\n" +
                "                                \"Action\": {\n" +
                "                                    \"action\": \"debug\",\n" +
                "                                    \"action_args\": {\n" +
                "                                        \"msg\": \"First one matches\"\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        ],\n" +
                "                        \"enabled\": true\n" +
                "                    }\n" +
                "                },\n" +
                "                {\n" +
                "                    \"Rule\": {\n" +
                "                        \"name\": \"R2\",\n" +
                "                        \"condition\": {\n" +
                "                            \"AllCondition\": [\n" +
                "                                {\n" +
                "                                    \"EqualsExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \" " + (partialOnLeft ? "i" : "j") + " \"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"Integer\": 0\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                },\n" +
                "                                {\n" +
                "                                    \"EqualsExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"" + (partialOnLeft ? "j" : "i") + " \"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"Integer\": 0\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            ]\n" +
                "                        },\n" +
                "                        \"actions\": [\n" +
                "                            {\n" +
                "                                \"Action\": {\n" +
                "                                    \"action\": \"debug\",\n" +
                "                                    \"action_args\": {\n" +
                "                                        \"msg\": \"Second one matches\"\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        ],\n" +
                "                        \"enabled\": true\n" +
                "                    }\n" +
                "                }\n" +
                "            ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(rules);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\" : 0 }").join();
        assertThat(matchedRules).hasSize(1);
        assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R1");

        matchedRules = rulesExecutor.processEvents("{ \"j\" : 0 }").join();
        assertThat(matchedRules).hasSize(matchMultipleRules ? 1 : 0);

        if (matchMultipleRules) {
            // when multiple match is allowed i=0 should be retained and now used to also fire R2
            assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R2");
            // if both rules fired now the working memory should be empty
            assertThat(rulesExecutor.getAllFacts()).isEmpty();
        } else {
            // if R2 never fired j=0 should still be there
            assertThat(rulesExecutor.getAllFacts().size()).isEqualTo(1);
        }

        rulesExecutor.dispose();
    }

    @Test
    public void retainLeftPartialMatchesWithMatchMultipleRulesWithPeer() {
        checkPartialMatchesWithMatchMultipleRulesWithPeer(true, true);
    }

    @Test
    public void discardLeftPartialMatchesWithMatchMultipleRulesWithPeer() {
        checkPartialMatchesWithMatchMultipleRulesWithPeer(false, true);
    }

    @Test
    public void retainRightPartialMatchesWithMatchMultipleRulesWithPeer() {
        checkPartialMatchesWithMatchMultipleRulesWithPeer(true, false);
    }

    @Test
    public void discardRightPartialMatchesWithMatchMultipleRulesWithPeer() {
        checkPartialMatchesWithMatchMultipleRulesWithPeer(false, false);
    }

    private static void checkPartialMatchesWithMatchMultipleRulesWithPeer(boolean matchMultipleRules, boolean partialOnLeft) {
        String rules =
                "{\n" +
                        "   \"match_multiple_rules\":" + matchMultipleRules + ",\n" +
                        "            \"rules\": [\n" +
                        "                {\n" +
                        "                    \"Rule\": {\n" +
                        "                        \"name\": \"R1\",\n" +
                        "                        \"condition\": {\n" +
                        "                            \"AllCondition\": [\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"i\"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            ]\n" +
                        "                        },\n" +
                        "                        \"actions\": [\n" +
                        "                            {\n" +
                        "                                \"Action\": {\n" +
                        "                                    \"action\": \"debug\",\n" +
                        "                                    \"action_args\": {\n" +
                        "                                        \"msg\": \"First one matches\"\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            }\n" +
                        "                        ],\n" +
                        "                        \"enabled\": true\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Rule\": {\n" +
                        "                        \"name\": \"R2\",\n" +
                        "                        \"condition\": {\n" +
                        "                            \"AllCondition\": [\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \" " + (partialOnLeft ? "i" : "j") + " \"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                },\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"" + (partialOnLeft ? "j" : "i") + " \"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            ]\n" +
                        "                        },\n" +
                        "                        \"actions\": [\n" +
                        "                            {\n" +
                        "                                \"Action\": {\n" +
                        "                                    \"action\": \"debug\",\n" +
                        "                                    \"action_args\": {\n" +
                        "                                        \"msg\": \"Second one matches\"\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            }\n" +
                        "                        ],\n" +
                        "                        \"enabled\": true\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Rule\": {\n" +
                        "                        \"name\": \"R3\",\n" +
                        "                        \"condition\": {\n" +
                        "                            \"AllCondition\": [\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \" " + (partialOnLeft ? "i" : "j") + " \"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                },\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"" + (partialOnLeft ? "j" : "i") + " \"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                },\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"k\"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            ]\n" +
                        "                        },\n" +
                        "                        \"actions\": [\n" +
                        "                            {\n" +
                        "                                \"Action\": {\n" +
                        "                                    \"action\": \"debug\",\n" +
                        "                                    \"action_args\": {\n" +
                        "                                        \"msg\": \"Third one matches\"\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            }\n" +
                        "                        ],\n" +
                        "                        \"enabled\": true\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            ]\n" +
                        "        }";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(rules);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\" : 0 }").join();
        assertThat(matchedRules).hasSize(1);
        assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R1");

        matchedRules = rulesExecutor.processEvents("{ \"j\" : 0 }").join();
        assertThat(matchedRules).hasSize(matchMultipleRules ? 1 : 0);

        if (matchMultipleRules) {
            // when multiple match is allowed i=0 should be retained and now used to also fire R2
            assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R2");
            // i=0 and j=0 have a partial match for R3. Retained
            assertThat(rulesExecutor.getAllFacts()).hasSize(2);
        } else {
            // if R2 never fired j=0 should still be there
            assertThat(rulesExecutor.getAllFacts().size()).isEqualTo(1);
        }

        matchedRules = rulesExecutor.processEvents("{ \"k\" : 0 }").join();
        assertThat(matchedRules).hasSize(matchMultipleRules ? 1 : 0);

        if (matchMultipleRules) {
            // when multiple match is allowed i=0 and j=0 should be retained and now used to also fire R3
            assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R3");
            // if all rules fired now the working memory should be empty
            assertThat(rulesExecutor.getAllFacts()).isEmpty();
        } else {
            // if R2 and R3 never fired j=0 and k=0 should still be there
            assertThat(rulesExecutor.getAllFacts().size()).isEqualTo(2);
        }

        rulesExecutor.dispose();
    }

    @Test
    public void retainPartialMatchesWithMatchMultipleRulesWithMultiplePeers() {
        String rules =
                "{\n" +
                        "   \"match_multiple_rules\": true,\n" +
                        "            \"rules\": [\n" +
                        "                {\n" +
                        "                    \"Rule\": {\n" +
                        "                        \"name\": \"R1\",\n" +
                        "                        \"condition\": {\n" +
                        "                            \"AllCondition\": [\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"i\"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            ]\n" +
                        "                        },\n" +
                        "                        \"actions\": [\n" +
                        "                            {\n" +
                        "                                \"Action\": {\n" +
                        "                                    \"action\": \"debug\",\n" +
                        "                                    \"action_args\": {\n" +
                        "                                        \"msg\": \"First one matches\"\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            }\n" +
                        "                        ],\n" +
                        "                        \"enabled\": true\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Rule\": {\n" +
                        "                        \"name\": \"R2\",\n" +
                        "                        \"condition\": {\n" +
                        "                            \"AllCondition\": [\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"i\"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                },\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"j\"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            ]\n" +
                        "                        },\n" +
                        "                        \"actions\": [\n" +
                        "                            {\n" +
                        "                                \"Action\": {\n" +
                        "                                    \"action\": \"debug\",\n" +
                        "                                    \"action_args\": {\n" +
                        "                                        \"msg\": \"Second one matches\"\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            }\n" +
                        "                        ],\n" +
                        "                        \"enabled\": true\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Rule\": {\n" +
                        "                        \"name\": \"R3\",\n" +
                        "                        \"condition\": {\n" +
                        "                            \"AllCondition\": [\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"i\"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                },\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"j\"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                },\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"k\"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            ]\n" +
                        "                        },\n" +
                        "                        \"actions\": [\n" +
                        "                            {\n" +
                        "                                \"Action\": {\n" +
                        "                                    \"action\": \"debug\",\n" +
                        "                                    \"action_args\": {\n" +
                        "                                        \"msg\": \"Third one matches\"\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            }\n" +
                        "                        ],\n" +
                        "                        \"enabled\": true\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Rule\": {\n" +
                        "                        \"name\": \"R4\",\n" +
                        "                        \"condition\": {\n" +
                        "                            \"AllCondition\": [\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"i\"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                },\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"j\"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                },\n" +
                        "                                {\n" +
                        "                                    \"EqualsExpression\": {\n" +
                        "                                        \"lhs\": {\n" +
                        "                                            \"Event\": \"l\"\n" +
                        "                                        },\n" +
                        "                                        \"rhs\": {\n" +
                        "                                            \"Integer\": 0\n" +
                        "                                        }\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            ]\n" +
                        "                        },\n" +
                        "                        \"actions\": [\n" +
                        "                            {\n" +
                        "                                \"Action\": {\n" +
                        "                                    \"action\": \"debug\",\n" +
                        "                                    \"action_args\": {\n" +
                        "                                        \"msg\": \"Fourth one matches\"\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            }\n" +
                        "                        ],\n" +
                        "                        \"enabled\": true\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            ]\n" +
                        "        }";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(rules);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\" : 0 }").join();
        assertThat(matchedRules).hasSize(1);
        assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R1");

        matchedRules = rulesExecutor.processEvents("{ \"j\" : 0 }").join();
        assertThat(matchedRules).hasSize(1);

        // when multiple match is allowed i=0 should be retained and now used to also fire R2
        assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R2");
        // i=0 and j=0 have a partial match for R3. Retained
        assertThat(rulesExecutor.getAllFacts()).hasSize(2);

        matchedRules = rulesExecutor.processEvents("{ \"k\" : 0 }").join();
        assertThat(matchedRules).hasSize(1);

        // when multiple match is allowed i=0 and j=0 should be retained and now used to also fire R3
        assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R3");
        // i=0 and j=0 have a partial match for R3. Retained. But k=0 is discarded
        assertThat(rulesExecutor.getAllFacts()).hasSize(2);

        matchedRules = rulesExecutor.processEvents("{ \"l\" : 0 }").join();
        assertThat(matchedRules).hasSize(1);

        // when multiple match is allowed i=0 and j=0 should be retained and now used to also fire R4
        assertThat(matchedRules.stream().map(m -> m.getRule().getName())).contains("R4");
        // if all rules fired now the working memory should be empty
        assertThat(rulesExecutor.getAllFacts()).isEmpty();

        rulesExecutor.dispose();
    }
}
