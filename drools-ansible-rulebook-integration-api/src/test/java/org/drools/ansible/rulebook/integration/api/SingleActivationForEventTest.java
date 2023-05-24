package org.drools.ansible.rulebook.integration.api;

import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class SingleActivationForEventTest {

    @Test
    public void test() {
        String json =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "          \"Rule\": {\n" +
                "            \"name\": \"R1\",\n" +
                "            \"condition\": {\n" +
                "              \"AllCondition\": [\n" +
                "                {\n" +
                "                  \"SelectAttrExpression\": {\n" +
                "                    \"lhs\": {\n" +
                "                      \"Event\": \"planets\"\n" +
                "                    },\n" +
                "                    \"rhs\": {\n" +
                "                      \"key\": {\n" +
                "                        \"String\": \"planet.radius\"\n" +
                "                      },\n" +
                "                      \"operator\": {\n" +
                "                        \"String\": \"<\"\n" +
                "                      },\n" +
                "                      \"value\": {\n" +
                "                        \"Float\": 1200.05\n" +
                "                      }\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              ]\n" +
                "            },\n" +
                "            \"actions\": [\n" +
                "              {\n" +
                "                \"Action\": {\n" +
                "                  \"action\": \"debug\",\n" +
                "                  \"action_args\": {\n" +
                "                    \"msg\": \"Output for testcase #04\"\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            ],\n" +
                "            \"enabled\": true\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"Rule\": {\n" +
                "            \"name\": \"R2\",\n" +
                "            \"condition\": {\n" +
                "              \"AllCondition\": [\n" +
                "                {\n" +
                "                  \"SelectAttrExpression\": {\n" +
                "                    \"lhs\": {\n" +
                "                      \"Event\": \"planets\"\n" +
                "                    },\n" +
                "                    \"rhs\": {\n" +
                "                      \"key\": {\n" +
                "                        \"String\": \"planet.radius\"\n" +
                "                      },\n" +
                "                      \"operator\": {\n" +
                "                        \"String\": \">=\"\n" +
                "                      },\n" +
                "                      \"value\": {\n" +
                "                        \"Float\": 1188.3\n" +
                "                      }\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              ]\n" +
                "            },\n" +
                "            \"actions\": [\n" +
                "              {\n" +
                "                \"Action\": {\n" +
                "                  \"action\": \"debug\",\n" +
                "                  \"action_args\": {\n" +
                "                    \"msg\": \"Output for testcase #05\"\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            ],\n" +
                "            \"enabled\": true\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"id\": \"testcase 04\", \"planets\": [ { \"planet\": { \"name\": \"venus\", \"radius\": 1200.01, \"moons\": null, \"is_planet\": true  } } ] }\n" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        SessionStats stats = rulesExecutor.dispose();
        assertEquals( 2, stats.getNumberOfRules() );
        assertEquals( 1, stats.getRulesTriggered() );
        assertEquals( 1, stats.getEventsProcessed() );
        assertEquals( 1, stats.getEventsMatched() );
        assertEquals( 0, stats.getEventsSuppressed() );
    }
}
