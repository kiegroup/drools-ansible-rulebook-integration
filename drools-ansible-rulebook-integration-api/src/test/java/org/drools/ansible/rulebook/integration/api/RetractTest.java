package org.drools.ansible.rulebook.integration.api;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RetractTest {

    @Test
    public void testExecuteRules() {
        String JSON1 =
                "{\n" +
                "           \"rules\": [\n" +
                "            {\n" +
                "                \"Rule\": {\n" +
                "                    \"action\": {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"assert_fact\",\n" +
                "                            \"action_args\": {\n" +
                "                                \"fact\": {\n" +
                "                                    \"msg\": \"hello world\"\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"condition\": {\n" +
                "                        \"AllCondition\": [\n" +
                "                            {\n" +
                "                                \"EqualsExpression\": {\n" +
                "                                    \"lhs\": {\n" +
                "                                        \"Event\": \"i\"\n" +
                "                                    },\n" +
                "                                    \"rhs\": {\n" +
                "                                        \"Integer\": 1\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    },\n" +
                "                    \"enabled\": true,\n" +
                "                    \"name\": null\n" +
                "                }\n" +
                "            },\n" +
                "            {\n" +
                "                \"Rule\": {\n" +
                "                    \"action\": {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"retract_fact\",\n" +
                "                            \"action_args\": {\n" +
                "                                \"fact\": {\n" +
                "                                    \"msg\": \"hello world\"\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"condition\": {\n" +
                "                        \"AllCondition\": [\n" +
                "                            {\n" +
                "                                \"EqualsExpression\": {\n" +
                "                                    \"lhs\": {\n" +
                "                                        \"Event\": \"msg\"\n" +
                "                                    },\n" +
                "                                    \"rhs\": {\n" +
                "                                        \"String\": \"hello world\"\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    },\n" +
                "                    \"enabled\": true,\n" +
                "                    \"name\": null\n" +
                "                }\n" +
                "            },\n" +
                "            {\n" +
                "                \"Rule\": {\n" +
                "                    \"action\": {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"debug\",\n" +
                "                            \"action_args\": {}\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"condition\": {\n" +
                "                        \"AllCondition\": [\n" +
                "                            {\n" +
                "                                \"IsNotDefinedExpression\": {\n" +
                "                                    \"Event\": \"msg\"\n" +
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"msg\" : \"hello world\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processRetractMatchingFacts( "{ \"msg\" : \"hello world\" }", false ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_2", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testRetract() {
        String JSON1 =
                "{\n" +
                "           \"rules\": [\n" +
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
                "                                        \"Integer\": 0\n" +
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"facts\" : [ { \"i\" : 1 }, { \"i\" : 1, \"j\" : 1 }, { \"i\" : 2, \"j\" : 2 } ] }" ).join();
        assertEquals( 3, matchedRules.size() );
        assertEquals( 3, rulesExecutor.getAllFacts().size() );

        matchedRules = rulesExecutor.processRetractMatchingFacts( "{ \"i\" : 1 }", false ).join();
        assertEquals( 0, matchedRules.size() );
        assertEquals( 2, rulesExecutor.getAllFacts().size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testRetractMatchingFacts() {
        String JSON1 =
                "{\n" +
                "           \"rules\": [\n" +
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
                "                                        \"Integer\": 0\n" +
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"facts\" : [ { \"i\" : 1 }, { \"i\" : 1, \"j\" : 1 }, { \"i\" : 2, \"j\" : 2 } ] }" ).join();
        assertEquals( 3, matchedRules.size() );
        assertEquals( 3, rulesExecutor.getAllFacts().size() );

        matchedRules = rulesExecutor.processRetractMatchingFacts( "{ \"i\" : 1 }", true ).join();
        assertEquals( 0, matchedRules.size() );
        assertEquals( 1, rulesExecutor.getAllFacts().size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testRetractMatchingFactsIgnoringKeys() {
        String JSON1 =
                "{\n" +
                "           \"rules\": [\n" +
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
                "                                        \"Integer\": 0\n" +
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"facts\" : [ " +
                "{ \"i\" : 1, \"x\" : 1, \"y\" : 1 }, " +
                "{ \"i\" : 1, \"x\" : { \"i\" : 1, \"j\" : 1 } }, " +
                "{ \"i\" : 1, \"j\" : 1 }, " +
                "{ \"i\" : 2, \"x\" : 1 } ] }" ).join();

        assertEquals( 4, matchedRules.size() );
        assertEquals( 4, rulesExecutor.getAllFacts().size() );

        matchedRules = rulesExecutor.processRetractMatchingFacts( "{ \"i\" : 1 }", false, "x", "y" ).join();
        assertEquals( 0, matchedRules.size() );
        assertEquals( 2, rulesExecutor.getAllFacts().size() );

        rulesExecutor.dispose();
    }
}
