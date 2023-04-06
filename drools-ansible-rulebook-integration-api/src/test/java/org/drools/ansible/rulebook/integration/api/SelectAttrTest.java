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
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"people\": [ { \"person\": { \"name\": \"Wilma\", \"age\": 23 } }, " +
                "{ \"person\": { \"name\": \"Betty\", \"age\": 25 } } ] }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"people\": [ { \"person\": { \"name\": \"Wilma\", \"age\": 43 } }, " +
                "{ \"person\": { \"name\": \"Betty\", \"age\": 45 } } ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSelectAttrOnSingleItem() {

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
                "                                    \"Event\": \"person\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"key\": {\n" +
                "                                        \"String\": \"age\"\n" +
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"person\": { \"name\": \"Fred\", \"age\": 54 } }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"person\": { \"name\": \"Wilma\", \"age\": 23 } }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testNegateSelectAttrOnSingleItem() {

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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"people\": { \"person\": { \"name\": \"Fred\", \"age\": 54 } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"people\": { \"person\": { \"name\": \"Wilma\", \"age\": 23 } } }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"people\": { \"man\": { \"name\": \"Barney\" } } }" ).join();
        assertEquals( 0, matchedRules.size() );

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

    @Test
    public void testSelectAttrNegated() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"Go\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"SelectAttrNotExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"my_obj\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"key\": {\n" +
                "                                        \"String\": \"thing.size\"\n" +
                "                                    },\n" +
                "                                    \"operator\": {\n" +
                "                                        \"String\": \">=\"\n" +
                "                                    },\n" +
                "                                    \"value\": {\n" +
                "                                        \"Integer\": 50\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                },\n" +
                "                \"actions\": [\n" +
                "                    {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"print_event\",\n" +
                "                            \"action_args\": {}\n" +
                "                        }\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"my_obj\": [ { \"thing\": { \"name\": \"a\", \"size\": 51 } }," +
                "{ \"thing\": { \"name\": \"b\", \"size\": 31 } }," +
                "{ \"thing\": { \"name\": \"c\", \"size\": 89 } } ] }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"my_obj\": [ { \"thing\": { \"name\": \"a\", \"size\": 51 } }," +
                "{ \"thing\": { \"name\": \"b\", \"size\": 61 } }," +
                "{ \"thing\": { \"name\": \"c\", \"size\": 89 } } ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSelectAttrIncompatibleTypes() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"Go\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"SelectAttrExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"my_obj\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"key\": {\n" +
                "                                        \"String\": \"thing.size\"\n" +
                "                                    },\n" +
                "                                    \"operator\": {\n" +
                "                                        \"String\": \">=\"\n" +
                "                                    },\n" +
                "                                    \"value\": {\n" +
                "                                        \"Integer\": 50\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                },\n" +
                "                \"actions\": [\n" +
                "                    {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"print_event\",\n" +
                "                            \"action_args\": {}\n" +
                "                        }\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"my_obj\": [ { \"thing\": { \"name\": \"a\", \"size\": \"large\" } }," +
                "{ \"thing\": { \"name\": \"b\", \"size\": \"medium\" } }," +
                "{ \"thing\": { \"name\": \"c\", \"size\": \"small\" } } ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSelectAttrWithScientificNotation() {

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
                "                                        \"String\": \"contains\"\n" +
                "                                    },\n" +
                "                                    \"value\": {\n" +
                "                                        \"Integer\": 1.021e+3\n" +
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
                "{ \"person\": { \"name\": \"Betty\", \"age\": 1021 } } ] }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"people\": [ { \"person\": { \"name\": \"Wilma\", \"age\": 23 } }, " +
                "{ \"person\": { \"name\": \"Betty\", \"age\": 25 } } ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }
}
