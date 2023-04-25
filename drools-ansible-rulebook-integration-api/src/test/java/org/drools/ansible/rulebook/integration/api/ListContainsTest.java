package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class ListContainsTest {

    @Test
    public void testListContainsInt() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "            {\n" +
                "                    \"Rule\": {\n" +
                "                        \"name\": \"contains_rule_int\",\n" +
                "                        \"condition\": {\n" +
                "                            \"AllCondition\": [\n" +
                "                                {\n" +
                "                                    \"ListContainsItemExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"id_list\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"Integer\": 1\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            ]\n" +
                "                        },\n" +
                "                        \"action\": {\n" +
                "                            \"Action\": {\n" +
                "                                \"action\": \"debug\",\n" +
                "                                \"action_args\": {}\n" +
                "                            }\n" +
                "                        },\n" +
                "                        \"enabled\": true\n" +
                "                    }\n" +
                "                }\n" +
                "        ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [2,4] }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [1,3,5] }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : 2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : 1 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testListNotContainsString() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "            {\n" +
                "                    \"Rule\": {\n" +
                "                        \"name\": \"contains_rule_int\",\n" +
                "                        \"condition\": {\n" +
                "                            \"AllCondition\": [\n" +
                "                                {\n" +
                "                                    \"ListNotContainsItemExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"friends\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"String\": \"pebbles\"\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            ]\n" +
                "                        },\n" +
                "                        \"action\": {\n" +
                "                            \"Action\": {\n" +
                "                                \"action\": \"debug\",\n" +
                "                                \"action_args\": {}\n" +
                "                            }\n" +
                "                        },\n" +
                "                        \"enabled\": true\n" +
                "                    }\n" +
                "                }\n" +
                "        ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"friends\" : [\"fred\", \"pebbles\"] }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"friends\" : [\"fred\", \"barney\"] }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testIntInList() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "            {\n" +
                "                    \"Rule\": {\n" +
                "                        \"name\": \"in_rule_int\",\n" +
                "                        \"condition\": {\n" +
                "                            \"AllCondition\": [\n" +
                "                                {\n" +
                "                                    \"ItemInListExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"i\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": [\n" +
                "                                            {\n" +
                "                                                \"Integer\": 1\n" +
                "                                            },\n" +
                "                                            {\n" +
                "                                                \"Integer\": 2\n" +
                "                                            },\n" +
                "                                            {\n" +
                "                                                \"Integer\": 3\n" +
                "                                            }\n" +
                "                                        ]\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            ]\n" +
                "                        },\n" +
                "                        \"action\": {\n" +
                "                            \"Action\": {\n" +
                "                                \"action\": \"debug\",\n" +
                "                                \"action_args\": {}\n" +
                "                            }\n" +
                "                        },\n" +
                "                        \"enabled\": true\n" +
                "                    }\n" +
                "            }\n" +
                "        ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\" : 4 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\" : 3 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "in_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testIntNotInList() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "            {\n" +
                "                    \"Rule\": {\n" +
                "                        \"name\": \"not_in_rule_int\",\n" +
                "                        \"condition\": {\n" +
                "                            \"AllCondition\": [\n" +
                "                                {\n" +
                "                                    \"ItemNotInListExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"i\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": [\n" +
                "                                            {\n" +
                "                                                \"Integer\": 1\n" +
                "                                            },\n" +
                "                                            {\n" +
                "                                                \"Integer\": 2\n" +
                "                                            },\n" +
                "                                            {\n" +
                "                                                \"Integer\": 3\n" +
                "                                            }\n" +
                "                                        ]\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            ]\n" +
                "                        },\n" +
                "                        \"action\": {\n" +
                "                            \"Action\": {\n" +
                "                                \"action\": \"debug\",\n" +
                "                                \"action_args\": {}\n" +
                "                            }\n" +
                "                        },\n" +
                "                        \"enabled\": true\n" +
                "                    }\n" +
                "            }\n" +
                "        ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\" : 3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\" : 4 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "not_in_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testListContainsWithScientificNotation() {

        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "            {\n" +
                "                    \"Rule\": {\n" +
                "                        \"name\": \"contains_rule_int\",\n" +
                "                        \"condition\": {\n" +
                "                            \"AllCondition\": [\n" +
                "                                {\n" +
                "                                    \"ListContainsItemExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"id_list\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"Integer\": 1.021e+3\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            ]\n" +
                "                        },\n" +
                "                        \"action\": {\n" +
                "                            \"Action\": {\n" +
                "                                \"action\": \"debug\",\n" +
                "                                \"action_args\": {}\n" +
                "                            }\n" +
                "                        },\n" +
                "                        \"enabled\": true\n" +
                "                    }\n" +
                "                }\n" +
                "        ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [2,4] }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [3,1021] }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [3,1021.00] }" ).join(); // 1021.00 becomes BigDecimal("1021.00") by RulesModelUtil.asFactMap()
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testListContainsWithJoinCondition() {

        String JSON1 =
                "{\n" +
                "        \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"EqualsExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"otherlist.name\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"String\": \"Delete\"\n" +
                "                                }\n" +
                "                            }\n" +
                "                        },\n" +
                "                        {\n" +
                "                            \"ListContainsItemExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"thirdlist.rnames\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"Events\": \"m_0.otherlist.resource_name\"\n" +
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
                "        }\n" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"otherlist\": { \"name\": \"Delete\", \"resource_name\": \"fred\" } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"thirdlist\": { \"rnames\": [ \"fred\", \"barney\" ] } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }
}
