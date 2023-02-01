package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class SelectTest {

    @Test
    public void testSelect() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"SelectExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"levels\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"operator\": {\n" +
                "                                        \"String\": \">\"\n" +
                "                                    },\n" +
                "                                    \"value\": {\n" +
                "                                        \"Integer\": 25\n" +
                "                                    }\n" +
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
                "                                \"message\": \"Hurray\"\n" +
                "                            }\n" +
                "                        }\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Fred\", \"age\": 54, \"levels\": [ 10, 20, 30] }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Barney\", \"age\": 53, \"levels\": [ 11, 15, 16] }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSelectOnSingleItem() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"SelectExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"levels\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"operator\": {\n" +
                "                                        \"String\": \">\"\n" +
                "                                    },\n" +
                "                                    \"value\": {\n" +
                "                                        \"Integer\": 25\n" +
                "                                    }\n" +
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
                "                                \"message\": \"Hurray\"\n" +
                "                            }\n" +
                "                        }\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Fred\", \"age\": 54, \"levels\": 30 }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Barney\", \"age\": 53, \"levels\": 16 }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testNegatedSelect() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"SelectNotExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"levels\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"operator\": {\n" +
                "                                        \"String\": \">\"\n" +
                "                                    },\n" +
                "                                    \"value\": {\n" +
                "                                        \"Integer\": 25\n" +
                "                                    }\n" +
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
                "                                \"message\": \"Hurray\"\n" +
                "                            }\n" +
                "                        }\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Fred\", \"age\": 54, \"levels\": [ 10, 20, 30] }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Barney\", \"age\": 53, \"levels\": [ 11, 15, 16] }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSelectWithRegEx() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"SelectExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"addresses\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"operator\": {\n" +
                "                                        \"String\": \"regex\"\n" +
                "                                    },\n" +
                "                                    \"value\": {\n" +
                "                                        \"String\": \"Main St\"\n" +
                "                                    }\n" +
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
                "                                \"message\": \"Hurray\"\n" +
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
                "                            \"SelectNotExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"addresses\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"operator\": {\n" +
                "                                       \"String\": \"regex\"\n" +
                "                                     },\n" +
                "                                     \"value\": {\n" +
                "                                        \"String\": \"Major St\"\n" +
                "                                     }\n" +
                "                                  }\n" +
                "                              }\n" +
                "                          }\n" +
                "                    ]\n" +
                "                },\n" +
                "                \"actions\": [\n" +
                "                     {\n" +
                "                         \"Action\": {\n" +
                "                             \"action\": \"echo\",\n" +
                "                             \"action_args\": {\n" +
                "                                 \"message\": \"No one lives on Major St\"\n" +
                "                             }\n" +
                "                         }\n" +
                "                      }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "             }\n" +
                "         }" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Fred\", \"age\": 54, \"addresses\": [ \"123 Main St, Bedrock, MI\", \"545 Spring St, Cresskill, NJ\", \"435 Wall Street, New York, NY\"] }" ).join();
        assertEquals( 2, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Barney\", \"age\": 53, \"addresses\": [ \"432 Raymond Blvd, Newark, NJ\", \"145 Wall St, Dumont, NJ\"] }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }
}
