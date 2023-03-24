package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class AlphaTest {

    @Test
    public void testEqualsWithFixedValue() {
        String json =
                "{\n" +
                "    \"rules\": [\n" +
                "            {\n" +
                "                \"Rule\": {\n" +
                "                    \"condition\": {\n" +
                "                        \"AllCondition\": [\n" +
                "                            {\n" +
                "                                \"EqualsExpression\": {\n" +
                "                                    \"lhs\": {\n" +
                "                                        \"Event\": \"j\"\n" +
                "                                    },\n" +
                "                                    \"rhs\": {\n" +
                "                                        \"Integer\": \"3\"\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    },\n" +
                "                    \"enabled\": true,\n" +
                "                    \"name\": null\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": 3 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 2 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 3 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());

        rulesExecutor.dispose();
    }

    @Test
    public void testEqualsOn2Fields() {
        String json =
                "{\n" +
                "    \"rules\": [\n" +
                "            {\n" +
                "                \"Rule\": {\n" +
                "                    \"condition\": {\n" +
                "                        \"AllCondition\": [\n" +
                "                            {\n" +
                "                                \"EqualsExpression\": {\n" +
                "                                    \"lhs\": {\n" +
                "                                        \"Event\": \"i\"\n" +
                "                                    },\n" +
                "                                    \"rhs\": {\n" +
                "                                        \"Event\": \"j\"\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    },\n" +
                "                    \"enabled\": true,\n" +
                "                    \"name\": null\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": 3 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 2 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 3 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());

        rulesExecutor.dispose();
    }

    @Test
    public void testGreaterOn2Fields() {
        String json =
                "{\n" +
                "    \"rules\": [\n" +
                "            {\n" +
                "                \"Rule\": {\n" +
                "                    \"condition\": {\n" +
                "                        \"AllCondition\": [\n" +
                "                            {\n" +
                "                                \"GreaterThanExpression\": {\n" +
                "                                    \"lhs\": {\n" +
                "                                        \"Event\": \"i\"\n" +
                "                                    },\n" +
                "                                    \"rhs\": {\n" +
                "                                        \"Event\": \"j\"\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    },\n" +
                "                    \"enabled\": true,\n" +
                "                    \"name\": null\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": 3 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 3 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3, \"j\": 2 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());

        rulesExecutor.dispose();
    }

    @Test
    public void testListContainsField() {

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
                "                                            \"Event\": \"i\"\n" +
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

        rulesExecutor.processFacts( "{ \"id_list\" : [2,4], \"i\" : 3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [1,3,5], \"i\" : 3 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : 2, \"i\" : 3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : 3, \"i\" : 3 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }
}
