package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class GetAllFactsTest {

    @Test
    public void test() {
        String json =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"EqualsExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"i\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"Integer\": 0\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                },\n" +
                "                \"actions\": [\n" +
                "                    {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"set_fact\",\n" +
                "                            \"action_args\": {\n" +
                "                                \"fact\": {\n" +
                "                                    \"status\": \"created\"\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r2\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"EqualsExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"i\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"Integer\": 4\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                },\n" +
                "                \"actions\": [\n" +
                "                    {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"debug\",\n" +
                "                            \"action_args\": {}\n" +
                "                        }\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r3\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"EqualsExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Fact\": \"status\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"String\": \"created\"\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                },\n" +
                "                \"actions\": [\n" +
                "                    {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"echo\",\n" +
                "                            \"action_args\": {\n" +
                "                                \"message\": \"Fact matches\"\n" +
                "                            }\n" +
                "                        }\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": 0 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r1", matchedRules.get(0).getRule().getName());
        assertEquals(0, rulesExecutor.getAllFacts().size());

        matchedRules = rulesExecutor.processFacts("{ \"status\": \"created\" }").join();
        assertEquals(1, matchedRules.size());
        assertEquals(1, rulesExecutor.getAllFacts().size());

        rulesExecutor.dispose();
    }
}
