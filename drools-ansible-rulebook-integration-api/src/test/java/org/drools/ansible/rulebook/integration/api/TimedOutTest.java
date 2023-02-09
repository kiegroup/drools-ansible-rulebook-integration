package org.drools.ansible.rulebook.integration.api;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.drools.core.facttemplates.Fact;
import org.json.JSONObject;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.api.RulesExecutorFactory.DEFAULT_AUTOMATIC_TICK_PERIOD_IN_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TimedOutTest {

    @Test
    public void timedOutTest() {
        String json =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"condition\":{\n" +
                "               \"NotAllCondition\":[\n" +
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
                "                     \"AssignmentExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Events\":\"myevent\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"EqualsExpression\":{\n" +
                "                              \"lhs\":{\n" +
                "                                 \"Event\":\"sensu.process.status\"\n" +
                "                              },\n" +
                "                              \"rhs\":{\n" +
                "                                 \"String\":\"stopped\"\n" +
                "                              }\n" +
                "                           }\n" +
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
                "               \"timeout\":\"10 seconds\"\n" +
                "            }\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

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
        assertEquals( "myevent", matchedRules.get(0).getDeclarationIds().get(0) );
        assertNotNull( matchedRules.get(0).getDeclarationValue("myevent") );

        SessionStats stats = rulesExecutor.dispose();
        assertEquals( 1, stats.getNumberOfRules() );
        assertEquals( 1, stats.getRulesTriggered() );
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
                "               \"NotAllCondition\":[\n" +
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
                "               ],\n" +
                "               \"timeout\":\"10 seconds\"\n" +
                "            },\n" +
                "            \"enabled\":true,\n" +
                "            \"name\":\"r1\"\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        // pseudo clock should be automatically activated by the presence of the timed_out
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"j\": 1 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 8, TimeUnit.SECONDS ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 3, TimeUnit.SECONDS ).join();
        assertEquals( 1, matchedRules.size() );
        Match match = matchedRules.get(0);
        assertEquals("r1", match.getRule().getName());
        assertEquals( 1, match.getDeclarationIds().size() );
        assertEquals( "m_1", match.getDeclarationIds().get(0) );
        assertEquals( 1, ((Fact) match.getDeclarationValue("m_1")).get("j") );
    }

    @Test
    public void testTimedOutWithAutomaticClockAdvance() throws IOException {
        String json =
                "{\n" +
                "       \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"maint failed\",\n" +
                "                \"condition\": {\n" +
                "                    \"NotAllCondition\": [\n" +
                "                        {\n" +
                "                            \"EqualsExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"alert.code\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"Integer\": 1001\n" +
                "                                }\n" +
                "                            }\n" +
                "                        },\n" +
                "                        {\n" +
                "                            \"EqualsExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"Event\": \"alert.code\"\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"Integer\": 1002\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ],\n" +
                "                    \"timeout\": \"2 seconds\"\n" +
                "                },\n" +
                "                \"action\": {\n" +
                "                    \"Action\": {\n" +
                "                        \"action\": \"print_event\",\n" +
                "                        \"action_args\": {}\n" +
                "                    }\n" +
                "                },\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n\n" +
                "}";

        RulesExecutorContainer rulesExecutorContainer = new RulesExecutorContainer();
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, json);
        assertTrue( rulesSet.hasAsyncExecution() );

        rulesSet.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK);
        rulesExecutorContainer.allowAsync();
        RulesExecutor rulesExecutor = rulesExecutorContainer.register( RulesExecutorFactory.createRulesExecutor(rulesSet) );

        int port = rulesExecutorContainer.port();

        try (Socket socket = new Socket("localhost", port)) {
            DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream());

            long assertTime = System.currentTimeMillis();
            List<Match> matchedRules = rulesExecutor.processEvents( "{ \"alert\": { \"code\": 1001, \"message\": \"Applying maintenance\" } }" ).join();
            assertEquals( 0, matchedRules.size() );

            // blocking call
            int l = bufferedInputStream.readInt();

            // fires after at least 2 seconds
            long elapsed = System.currentTimeMillis() - assertTime;
            assertTrue("rule fired after " + elapsed + " milliseconds", elapsed >= 2000);

            byte[] bytes = bufferedInputStream.readNBytes(l);
            String r = new String(bytes, StandardCharsets.UTF_8);
            JSONObject v = new JSONObject(r);

            List<Object> matches = v.getJSONArray("result").toList();
            Map<String, Map> match = (Map<String, Map>) matches.get(0);

            assertNotNull(match.get("maint failed"));
        } finally {
            SessionStats stats = rulesExecutorContainer.disposeAll();
            assertEquals(1, stats.getAsyncResponses());
            assertTrue(stats.getBytesSentOnAsync() > 100);
        }
    }
}
