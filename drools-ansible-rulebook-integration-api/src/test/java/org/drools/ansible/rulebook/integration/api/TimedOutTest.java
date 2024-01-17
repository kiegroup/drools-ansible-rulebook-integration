package org.drools.ansible.rulebook.integration.api;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.junit.Test;
import org.kie.api.prototype.PrototypeFactInstance;
import org.kie.api.runtime.rule.Match;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.drools.ansible.rulebook.integration.api.RulesExecutorFactory.DEFAULT_AUTOMATIC_TICK_PERIOD_IN_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TimedOutTest {

    @Test
    public void timedOutTest() {
        String json =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "condition":{
                               "NotAllCondition":[
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"ping.timeout"
                                        },
                                        "rhs":{
                                           "Boolean":true
                                        }
                                     }
                                  },
                                  {
                                     "AssignmentExpression":{
                                        "lhs":{
                                           "Events":"myevent"
                                        },
                                        "rhs":{
                                           "EqualsExpression":{
                                              "lhs":{
                                                 "Event":"sensu.process.status"
                                              },
                                              "rhs":{
                                                 "String":"stopped"
                                              }
                                           }
                                        }
                                     }
                                  },
                                  {
                                     "GreaterThanExpression":{
                                        "lhs":{
                                           "Event":"sensu.storage.percent"
                                        },
                                        "rhs":{
                                           "Integer":95
                                        }
                                     }
                                  }
                               ],
                               "timeout":"10 seconds"
                            }
                         }
                      }
                   ]
                }
                """;

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

        assertEquals( 0, rulesExecutor.getSessionStats().getPermanentStorageCount() );

        // --- second round

        matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"status\":\"stopped\" } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.advanceTime( 8, TimeUnit.SECONDS );

        matchedRules = rulesExecutor.processEvents( "{ \"ping\": { \"timeout\": true } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 1, TimeUnit.SECONDS ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.advanceTime( 20, TimeUnit.SECONDS ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "myevent", matchedRules.get(0).getDeclarationIds().get(0) );
        assertNotNull( matchedRules.get(0).getDeclarationValue("myevent") );

        SessionStats stats = rulesExecutor.dispose();
        assertEquals( 1, stats.getNumberOfRules() );
        assertEquals( 1, stats.getRulesTriggered() );
        assertEquals( 0, stats.getPermanentStorageCount() );
    }

    @Test
    public void testSimpleTimedOut() {
        String json =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "action":{
                               "Action":{
                                  "action":"debug",
                                  "action_args":{
                                    \s
                                  }
                               }
                            },
                            "condition":{
                               "NotAllCondition":[
                                  {
                                     "IsDefinedExpression":{
                                        "Event":"i"
                                     }
                                  },
                                  {
                                     "IsDefinedExpression":{
                                        "Event":"j"
                                     }
                                  }
                               ],
                               "timeout":"10 seconds"
                            },
                            "enabled":true,
                            "name":"r1"
                         }
                      }
                   ]
                }
                """;

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
        assertEquals( 1, ((PrototypeFactInstance) match.getDeclarationValue("m_1")).get("j") );

        SessionStats stats = rulesExecutor.dispose();
        assertEquals(1, stats.getNumberOfRules());
        assertEquals(1, stats.getRulesTriggered());
        assertEquals(1, stats.getEventsMatched());
        assertEquals(1, stats.getEventsProcessed());
        assertEquals(0, stats.getEventsSuppressed());
        assertEquals(0, stats.getPermanentStorageCount());
    }
    
    @Test
    public void testTimedOutWithAutomaticClockAdvance() throws IOException {
        String json =
                """
                {
                       "rules": [
                        {
                            "Rule": {
                                "name": "maint failed",
                                "condition": {
                                    "NotAllCondition": [
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "alert.code"
                                                },
                                                "rhs": {
                                                    "Integer": 1001
                                                }
                                            }
                                        },
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "alert.code"
                                                },
                                                "rhs": {
                                                    "Integer": 1002
                                                }
                                            }
                                        }
                                    ],
                                    "timeout": "2 seconds"
                                },
                                "action": {
                                    "Action": {
                                        "action": "print_event",
                                        "action_args": {}
                                    }
                                },
                                "enabled": true
                            }
                        }
                    ]
                }
                """;

        RulesExecutorContainer rulesExecutorContainer = new RulesExecutorContainer();
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, json);
        assertTrue( rulesSet.hasAsyncExecution() );

        rulesSet.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK);
        rulesExecutorContainer.allowAsync();
        RulesExecutor rulesExecutor = rulesExecutorContainer.register( RulesExecutorFactory.createRulesExecutor(rulesSet) );

        int port = rulesExecutorContainer.port();

        try (Socket socket = new Socket("localhost", port);
             DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream())) {

            long assertTime = System.nanoTime();
            List<Match> matchedRules = rulesExecutor.processEvents( "{ \"alert\": { \"code\": 1001, \"message\": \"Applying maintenance\" } }" ).join();
            assertEquals( 0, matchedRules.size() );

            // blocking call
            int l = bufferedInputStream.readInt();

            // fires after at least 2 seconds
            long elapsed = (System.nanoTime() - assertTime) / 1_000_000;
            assertTrue("rule fired after " + elapsed + " milliseconds", elapsed >= 1_900);

            byte[] bytes = bufferedInputStream.readNBytes(l);
            String r = new String(bytes, StandardCharsets.UTF_8);

            List<Object> matches = JsonMapper.readValueExtractFieldAsList(r, "result");
            Map<String, Map> match = (Map<String, Map>) matches.get(0);

            assertNotNull(match.get("maint failed"));
        } finally {
            SessionStats stats = rulesExecutorContainer.disposeAll();
            assertEquals(0, stats.getPermanentStorageCount());
        }
    }
}
