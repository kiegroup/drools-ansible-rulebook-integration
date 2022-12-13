package org.drools.ansible.rulebook.integration.api;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.drools.core.facttemplates.Fact;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.api.RulesExecutorFactory.DEFAULT_AUTOMATIC_TICK_PERIOD_IN_MILLIS;
import static org.junit.Assert.assertEquals;

public class TimedOutTest {

    @Test
    public void testTimedOutInCondition() {
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

        timedOutTest(json);
    }

    @Test
    public void testTimedOutInRule() {
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

        timedOutTest(json);
    }

    private void timedOutTest(String json) {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // using a rule with a timed_out option automatically starts the scheduled pseudo clock
        assertEquals(DEFAULT_AUTOMATIC_TICK_PERIOD_IN_MILLIS, rulesExecutor.getAutomaticPseudoClockPeriod());

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"status\":\"stopped\" } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.advanceTime( 8, TimeUnit.SECONDS );

        matchedRules = rulesExecutor.processEvents( "{ \"ping\": { \"timeout\": true } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 1, TimeUnit.SECONDS ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"storage\": { \"percent\":97 } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 2, TimeUnit.SECONDS ).join();
        assertEquals( 0, matchedRules.size() );

        // --- second round

        rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"status\":\"stopped\" } } }" );
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.advanceTime( 8, TimeUnit.SECONDS );

        matchedRules = rulesExecutor.processEvents( "{ \"ping\": { \"timeout\": true } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 1, TimeUnit.SECONDS ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 2, TimeUnit.SECONDS ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSimpleTimedOut() {
        String json =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"action\":{\n" +
                "               \"Action\":{\n" +
                "                  \"action\":\"debug\",\n" +
                "                  \"action_args\":{\n" +
                "                     \n" +
                "                  }\n" +
                "               }\n" +
                "            },\n" +
                "            \"condition\":{\n" +
                "               \"AllCondition\":[\n" +
                "                  {\n" +
                "                     \"IsDefinedExpression\":{\n" +
                "                        \"Event\":\"i\"\n" +
                "                     }\n" +
                "                  },\n" +
                "                  {\n" +
                "                     \"IsDefinedExpression\":{\n" +
                "                        \"Event\":\"j\"\n" +
                "                     }\n" +
                "                  }\n" +
                "               ]\n" +
                "            },\n" +
                "            \"enabled\":true,\n" +
                "            \"name\":\"r1\",\n" +
                "            \"timed_out\":\"10 seconds\"\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        // pseudo clock should be automatically activated by the presence of the timed_out
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = matchedRules = rulesExecutor.processEvents( "{ \"j\": 1 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 8, TimeUnit.SECONDS ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 3, TimeUnit.SECONDS ).join();
        assertEquals( 1, matchedRules.size() );
        Match match = matchedRules.get(0);
        assertEquals("r1", match.getRule().getName());
        assertEquals( 1, ((Fact) match.getDeclarationValue("m")).get("j") );
    }
}
