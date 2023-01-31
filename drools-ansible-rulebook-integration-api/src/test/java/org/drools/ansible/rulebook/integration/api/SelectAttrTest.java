package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class SelectAttrTest {

    @Test
    public void testSelectAttr() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"SelectAttrExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"people\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"key\": {\n" +
                "                                        \"String\": \"person.age\"\n" +
                "                                    },\n" +
                "                                    \"operator\": {\n" +
                "                                        \"String\": \">\"\n" +
                "                                    },\n" +
                "                                    \"value\": {\n" +
                "                                        \"Integer\": 30\n" +
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
                "                                \"message\": \"Has a person greater than 30\"\n" +
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"people\": [ { \"person\": { \"name\": \"Fred\", \"age\": 54 } }, " +
                "{ \"person\": { \"name\": \"Barney\", \"age\": 45 } }, " +
                "{ \"person\": { \"name\": \"Wilma\", \"age\": 23 } }, " +
                "{ \"person\": { \"name\": \"Betty\", \"age\": 25 } } ] }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"people\": [ { \"person\": { \"name\": \"Wilma\", \"age\": 23 } }, " +
                "{ \"person\": { \"name\": \"Betty\", \"age\": 25 } } ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testNegateSelectAttr() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"SelectAttrNotExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"people\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"key\": {\n" +
                "                                        \"String\": \"person.age\"\n" +
                "                                    },\n" +
                "                                    \"operator\": {\n" +
                "                                        \"String\": \">\"\n" +
                "                                    },\n" +
                "                                    \"value\": {\n" +
                "                                        \"Integer\": 30\n" +
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
                "                                \"message\": \"Has a person greater than 30\"\n" +
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"people\": [ { \"person\": { \"name\": \"Fred\", \"age\": 54 } }, " +
                "{ \"person\": { \"name\": \"Barney\", \"age\": 45 } }, " +
                "{ \"person\": { \"name\": \"Wilma\", \"age\": 23 } }, " +
                "{ \"person\": { \"name\": \"Betty\", \"age\": 25 } } ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"people\": [ { \"person\": { \"name\": \"Wilma\", \"age\": 23 } }, " +
                "{ \"person\": { \"name\": \"Betty\", \"age\": 25 } } ] }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSelectAttrWithIn() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"SelectAttrExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"people\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"key\": {\n" +
                "                                        \"String\": \"person.age\"\n" +
                "                                    },\n" +
                "                                    \"operator\": {\n" +
                "                                        \"String\": \"in\"\n" +
                "                                    },\n" +
                "                                    \"value\": [" +
                "                                        {\n" +
                "                                            \"Integer\": 25\n" +
                "                                        }," +
                "                                        {\n" +
                "                                            \"Integer\": 55\n" +
                "                                        }" +
                "                                    ]\n" +
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
                "                                \"message\": \"Has a person greater than 30\"\n" +
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"people\": [ { \"person\": { \"name\": \"Fred\", \"age\": 54 } }, " +
                "{ \"person\": { \"name\": \"Barney\", \"age\": 45 } }, " +
                "{ \"person\": { \"name\": \"Wilma\", \"age\": 23 } }, " +
                "{ \"person\": { \"name\": \"Betty\", \"age\": 25 } } ] }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"people\": [ { \"person\": { \"name\": \"Wilma\", \"age\": 23 } }, " +
                "{ \"person\": { \"name\": \"Barney\", \"age\": 45 } } ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

}
