package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class IsNotDefinedTest {

    @Test
    public void testExecuteRules() {
        String JSON1 =
                "{\n" +
                "   \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"r1\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"AndExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"EqualsExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"i\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"Integer\": 1\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"IsNotDefinedExpression\": {\n" +
                "                                        \"Event\": \"j\"\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                },\n" +
                "                \"actions\": [\n" +
                "                    {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"debug\",\n" +
                "                            \"action_args\": {\n" +
                "                                \"msg\": \"Hello\"\n" +
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

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\": 1 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }
}
