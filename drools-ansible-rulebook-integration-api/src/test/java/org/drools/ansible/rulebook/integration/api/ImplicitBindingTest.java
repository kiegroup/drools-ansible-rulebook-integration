package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class ImplicitBindingTest {

    @Test
    public void testAllCondition() {
        String JSON1 =
                "  {\n" +
                "      \"rules\": [\n" +
                "        {\n" +
                "          \"Rule\": {\n" +
                "            \"condition\": {\n" +
                "              \"AllCondition\": [\n" +
                "                {\n" +
                "                  \"AndExpression\": {\n" +
                "                    \"lhs\": {\n" +
                "                      \"AndExpression\": {\n" +
                "                        \"lhs\": {\n" +
                "                          \"GreaterThanExpression\": {\n" +
                "                            \"lhs\": {\n" +
                "                              \"Event\": \"i\"\n" +
                "                            },\n" +
                "                            \"rhs\": {\n" +
                "                              \"Integer\": 0\n" +
                "                            }\n" +
                "                          }\n" +
                "                        },\n" +
                "                        \"rhs\": {\n" +
                "                          \"GreaterThanExpression\": {\n" +
                "                            \"lhs\": {\n" +
                "                              \"Event\": \"i\"\n" +
                "                            },\n" +
                "                            \"rhs\": {\n" +
                "                              \"Integer\": 1\n" +
                "                            }\n" +
                "                          }\n" +
                "                        }\n" +
                "                      }\n" +
                "                    },\n" +
                "                    \"rhs\": {\n" +
                "                      \"GreaterThanExpression\": {\n" +
                "                        \"lhs\": {\n" +
                "                          \"Event\": \"i\"\n" +
                "                        },\n" +
                "                        \"rhs\": {\n" +
                "                          \"Integer\": 3\n" +
                "                        }\n" +
                "                      }\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              ]\n" +
                "            },\n" +
                "            \"enabled\": true,\n" +
                "            \"name\": \"r1\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":67 }" ).join();
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );
        assertEquals( "m", matchedRules.get(0).getDeclarationIds().get(0) );
        rulesExecutor.dispose();
    }

    @Test
    public void testAnyCondition() {
        String JSON1 =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"condition\":{\n" +
                "               \"AnyCondition\":[\n" +
                "                  {\n" +
                "                     \"EqualsExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"i\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"Integer\":0\n" +
                "                        }\n" +
                "                     }\n" +
                "                  },\n" +
                "                  {\n" +
                "                     \"GreaterThanExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"i\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"Integer\":1\n" +
                "                        }\n" +
                "                     }\n" +
                "                  }\n" +
                "               ]\n" +
                "            },\n" +
                "            \"enabled\":true,\n" +
                "            \"name\":\"r1\"\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        System.out.println(JSON1);

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts("{ \"i\":1 }").join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":67 }" ).join();
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );
        assertEquals( "m_1", matchedRules.get(0).getDeclarationIds().get(0) );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":0 }" ).join();
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );
        assertEquals( "m_0", matchedRules.get(0).getDeclarationIds().get(0) );
        rulesExecutor.dispose();
    }
}
