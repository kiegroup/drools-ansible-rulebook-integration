package org.drools.ansible.rulebook.integration.api;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class AutoEvictTest {

    @Test
    public void testClockPeriod() {
        String json =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "condition":{
                               "AllCondition":[
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Fact":"i"
                                        },
                                        "rhs":{
                                           "Integer":2
                                        }
                                     }
                                  }
                               ]
                            },
                            "enabled":true,
                            "name":"r_0"
                         }
                      }
                   ],
                   "clock_period":"1 second"
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        assertEquals(1000, rulesExecutor.getAutomaticPseudoClockPeriod()); // 1 second

        List<Match> matchedRules = rulesExecutor.processFacts("{ \"i\": 3 }").join();
        assertEquals(0, matchedRules.size());
        assertEquals(0, rulesExecutor.getAllFacts().size());

        matchedRules = rulesExecutor.processFacts("{ \"i\": 2 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals(1, rulesExecutor.getAllFacts().size());

        rulesExecutor.dispose();
    }

    @Test
    public void testAutomaticEvictEventsOnFiring() {
        String json =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "condition":{
                               "AllCondition":[
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"i"
                                        },
                                        "rhs":{
                                           "Integer":2
                                        }
                                     }
                                  },
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"j"
                                        },
                                        "rhs":{
                                           "Event":"m_0.i"
                                        }
                                     }
                                  }
                               ]
                            },
                            "enabled":true,
                            "name":"r_0"
                         }
                      }
                   ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"j\": 2 }").join();
        assertEquals(0, matchedRules.size());
        assertEquals(1, rulesExecutor.getAllFacts().size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 2 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals(0, rulesExecutor.getAllFacts().size()); // rule firing automatically evicts all involved events

        rulesExecutor.dispose();
    }

    @Test
    public void testEvictEventsUsingDefaultExpiration() {
        String json =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "condition":{
                               "AllCondition":[
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"i"
                                        },
                                        "rhs":{
                                           "Integer":2
                                        }
                                     }
                                  },
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"j"
                                        },
                                        "rhs":{
                                           "Event":"m_0.i"
                                        }
                                     }
                                  }
                               ]
                            },
                            "enabled":true,
                            "name":"r_0"
                         }
                      }
                   ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"j\": 2 }").join();
        assertEquals(0, matchedRules.size());
        assertEquals(1, rulesExecutor.getAllFacts().size());

        // after 1 hour the event should be still there
        rulesExecutor.advanceTime( 1, TimeUnit.HOURS );
        assertEquals(1, rulesExecutor.getAllFacts().size());

        // after 1h59 the event should be still there
        rulesExecutor.advanceTime( 59, TimeUnit.MINUTES );
        assertEquals(1, rulesExecutor.getAllFacts().size());

        // after 2h01 the event shouldn't be there anymore
        rulesExecutor.advanceTime( 2, TimeUnit.MINUTES );
        assertEquals(0, rulesExecutor.getAllFacts().size());

        rulesExecutor.dispose();
    }

    @Test
    public void testEvictEventsUsingGivenExpiration() {
        String json =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "condition":{
                               "AllCondition":[
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"i"
                                        },
                                        "rhs":{
                                           "Integer":2
                                        }
                                     }
                                  },
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"j"
                                        },
                                        "rhs":{
                                           "Event":"m_0.i"
                                        }
                                     }
                                  }
                               ]
                            },
                            "enabled":true,
                            "name":"r_0"
                         }
                      }
                   ],
                   "default_events_ttl":"10 hours"
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"j\": 2 }").join();
        assertEquals(0, matchedRules.size());
        assertEquals(1, rulesExecutor.getAllFacts().size());

        // after 1 hour the event should be still there
        rulesExecutor.advanceTime( 1, TimeUnit.HOURS );
        assertEquals(1, rulesExecutor.getAllFacts().size());

        // after 1h59 the event should be still there
        rulesExecutor.advanceTime( 59, TimeUnit.MINUTES );
        assertEquals(1, rulesExecutor.getAllFacts().size());

        // after 2h01 the event should be still there
        rulesExecutor.advanceTime( 2, TimeUnit.MINUTES );
        assertEquals(1, rulesExecutor.getAllFacts().size());

        // after 10h01 the event shouldn't be there anymore
        rulesExecutor.advanceTime( 8, TimeUnit.HOURS );
        assertEquals(0, rulesExecutor.getAllFacts().size());

        rulesExecutor.dispose();
    }
}
