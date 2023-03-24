package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class BetaEqualsTest {

    @Test
    public void testExecuteRulesWithExplicitJoin() {
        String json =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"condition\":{\n" +
                "               \"AllCondition\":[\n" +
                "                  {\n" +
                "                     \"AssignmentExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Facts\":\"first\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"IsDefinedExpression\":{\n" +
                "                              \"Fact\":\"custom.expected_index\"\n" +
                "                           }\n" +
                "                        }\n" +
                "                     }\n" +
                "                  },\n" +
                "                  {\n" +
                "                     \"EqualsExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"i\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"Facts\":\"first.custom.expected_index\"\n" +
                "                        }\n" +
                "                     }\n" +
                "                  }\n" +
                "               ]\n" +
                "            },\n" +
                "            \"enabled\":true,\n" +
                "            \"name\":null\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts("{ \"custom\": { \"expected_index\": 2 } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 2 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());

        rulesExecutor.dispose();
    }
}