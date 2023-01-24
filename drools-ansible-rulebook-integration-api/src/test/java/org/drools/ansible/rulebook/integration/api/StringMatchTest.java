package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class StringMatchTest {

    @Test
    public void testMatch() {
        String json =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"name\":\"R1\",\n" +
                "            \"condition\":{\n" +
                "               \"AllCondition\":[\n" +
                "                  {\n" +
                "                     \"SearchMatchesExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"url1\"\n" +
                "                        },\n" +
                "                        \"rhs\": {\n" +
                "                            \"SearchType\": {\n" +
                "                                \"kind\": {\n" +
                "                                    \"String\": \"match\"\n" +
                "                                },\n" +
                "                                \"pattern\": {\n" +
                "                                    \"String\": \"https://example.com/users/.*/resources\"\n" +
                "                                },\n" +
                "                                \"options\": [\n" +
                "                                    {\n" +
                "                                        \"name\": {\n" +
                "                                            \"String\": \"ignorecase\"\n" +
                "                                        },\n" +
                "                                        \"value\": {\n" +
                "                                            \"Boolean\": true\n" +
                "                                        }\n" +
                "                                    },\n" +
                "                                    {\n" +
                "                                        \"name\": {\n" +
                "                                            \"String\": \"multiline\"\n" +
                "                                        },\n" +
                "                                        \"value\": {\n" +
                "                                            \"Boolean\": true\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                ]\n" +
                "                            }\n" +
                "                        }\n" +
                "                     }\n" +
                "                  }\n" +
                "               ]\n" +
                "            }\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.org/users/foo/resources/bar\" }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.com/users/foo/resources/bar\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.COM/users/foo/resources/bar\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testNotMatch() {
        String json =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"name\":\"R1\",\n" +
                "            \"condition\":{\n" +
                "               \"AllCondition\":[\n" +
                "                  {\n" +
                "                     \"SearchNotMatchesExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"url1\"\n" +
                "                        },\n" +
                "                        \"rhs\": {\n" +
                "                            \"SearchType\": {\n" +
                "                                \"kind\": {\n" +
                "                                    \"String\": \"match\"\n" +
                "                                },\n" +
                "                                \"pattern\": {\n" +
                "                                    \"String\": \"https://example.com/users/.*/resources\"\n" +
                "                                },\n" +
                "                                \"options\": [\n" +
                "                                    {\n" +
                "                                        \"name\": {\n" +
                "                                            \"String\": \"ignorecase\"\n" +
                "                                        },\n" +
                "                                        \"value\": {\n" +
                "                                            \"Boolean\": true\n" +
                "                                        }\n" +
                "                                    },\n" +
                "                                    {\n" +
                "                                        \"name\": {\n" +
                "                                            \"String\": \"multiline\"\n" +
                "                                        },\n" +
                "                                        \"value\": {\n" +
                "                                            \"Boolean\": true\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                ]\n" +
                "                            }\n" +
                "                        }\n" +
                "                     }\n" +
                "                  }\n" +
                "               ]\n" +
                "            }\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.org/users/foo/resources/bar\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.com/users/foo/resources/bar\" }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.COM/users/foo/resources/bar\" }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }
}
