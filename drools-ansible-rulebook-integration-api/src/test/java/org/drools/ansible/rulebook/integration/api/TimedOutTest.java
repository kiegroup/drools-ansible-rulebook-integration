package org.drools.ansible.rulebook.integration.api;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class TimedOutTest {

    @Test
    public void testTimeWindowInCondition() {
        String json =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"condition\":{\n" +
                "               \"AllCondition\":[\n" +
                "                  {\n" +
                "                     \"EqualsExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"ping.timeout\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"Boolean\":true\n" +
                "                        }\n" +
                "                     }\n" +
                "                  },\n" +
                "                  {\n" +
                "                     \"EqualsExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"sensu.process.status\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"String\":\"stopped\"\n" +
                "                        }\n" +
                "                     }\n" +
                "                  },\n" +
                "                  {\n" +
                "                     \"GreaterThanExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"sensu.storage.percent\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"Integer\":95\n" +
                "                        }\n" +
                "                     }\n" +
                "                  }\n" +
                "               ],\n" +
                "               \"timed_out\":\"10 seconds\"\n" +
                "            }\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        timeWindowTest(json);
    }

    @Test
    public void testTimeWindowInRule() {
        String json =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"condition\":{\n" +
                "               \"AllCondition\":[\n" +
                "                  {\n" +
                "                     \"EqualsExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"ping.timeout\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"Boolean\":true\n" +
                "                        }\n" +
                "                     }\n" +
                "                  },\n" +
                "                  {\n" +
                "                     \"EqualsExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"sensu.process.status\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"String\":\"stopped\"\n" +
                "                        }\n" +
                "                     }\n" +
                "                  },\n" +
                "                  {\n" +
                "                     \"GreaterThanExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"sensu.storage.percent\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"Integer\":95\n" +
                "                        }\n" +
                "                     }\n" +
                "                  }\n" +
                "               ]\n" +
                "            },\n" +
                "            \"timed_out\":\"10 seconds\"\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        timeWindowTest(json);
    }

    private void timeWindowTest(String json) {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);
        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"status\":\"stopped\" } } }" );
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.advanceTime( 8, TimeUnit.SECONDS );

        matchedRules = rulesExecutor.processEvents( "{ \"ping\": { \"timeout\": true } }" );
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 1, TimeUnit.SECONDS );
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"storage\": { \"percent\":97 } } }" );
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 2, TimeUnit.SECONDS );
        assertEquals( 0, matchedRules.size() );

        // --- second round

        rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"status\":\"stopped\" } } }" );
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.advanceTime( 8, TimeUnit.SECONDS );

        matchedRules = rulesExecutor.processEvents( "{ \"ping\": { \"timeout\": true } }" );
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 1, TimeUnit.SECONDS );
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 2, TimeUnit.SECONDS );
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }
}
