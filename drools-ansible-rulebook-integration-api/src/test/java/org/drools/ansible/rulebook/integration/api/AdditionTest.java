package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.model.Index;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.drools.model.PrototypeExpression.fixedValue;
import static org.drools.model.PrototypeExpression.prototypeField;
import static org.junit.Assert.assertEquals;

public class AdditionTest {

    @Test
    public void testExecuteRules() {
        String json =
                "{\n" +
                "    \"rules\": [\n" +
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
                "                                \"EqualsExpression\": {\n" +
                "                                    \"lhs\": {\n" +
                "                                        \"Event\": \"nested.i\"\n" +
                "                                    },\n" +
                "                                    \"rhs\": {\n" +
                "                                        \"AdditionExpression\": {\n" +
                "                                            \"lhs\": {\n" +
                "                                                \"Event\": \"nested.j\"\n" +
                "                                            },\n" +
                "                                            \"rhs\": {\n" +
                "                                                \"Integer\": 1\n" +
                "                                            }\n" +
                "                                        }\n" +
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"nested\": { \"i\": 1, \"j\":2 } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"nested\": { \"i\": 2, \"j\":1 } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testExecuteRulesSet() {
        RulesSet rulesSet = new RulesSet();
        rulesSet.addRule().withCondition().all()
                .addSingleCondition(prototypeField("nested.i"), Index.ConstraintType.EQUAL, prototypeField("nested.j").add(fixedValue(1)));

        RulesExecutor rulesExecutor = RulesExecutorFactory.createRulesExecutor(rulesSet);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"nested\": { \"i\": 1, \"j\":2 } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"nested\": { \"i\": 2, \"j\":1 } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testAdditionOnDifferentEvents() {
        String json =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"AssignmentExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Events\": \"abc\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"EqualsExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"i\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"Integer\": 1\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        },\n" +
                "                        {\n" +
                "                            \"EqualsExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"i\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"Integer\": 3\n" +
                "                                }\n" +
                "                            }\n" +
                "                        },\n" +
                "                        {\n" +
                "                            \"EqualsExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"i\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"AdditionExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Events\": \"abc.i\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"Events\": \"m_1.i\"\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\": 1 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"i\": 2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"i\": 3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"i\": 4 }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"i\": 5 }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }
}
