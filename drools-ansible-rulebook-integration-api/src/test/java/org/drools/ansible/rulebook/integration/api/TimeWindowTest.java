package org.drools.ansible.rulebook.integration.api;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimeWindowTest {

    @Test
    void testTimeWindowInCondition() {
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
                                           "Event":"ping.timeout"
                                        },
                                        "rhs":{
                                           "Boolean":true
                                        }
                                     }
                                  },
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"sensu.process.status"
                                        },
                                        "rhs":{
                                           "String":"stopped"
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

        timeWindowTest(json);
    }

    @Test
    void testTimeWindowInRule() {
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
                                           "Event":"ping.timeout"
                                        },
                                        "rhs":{
                                           "Boolean":true
                                        }
                                     }
                                  },
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"sensu.process.status"
                                        },
                                        "rhs":{
                                           "String":"stopped"
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
                               ]
                            },
                            "timeout":"10 seconds"
                         }
                      }
                   ]
                }
                """;

        timeWindowTest(json);
    }

    private void timeWindowTest(String json) {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);
        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"status\":\"stopped\" } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.advanceTime( 8, TimeUnit.SECONDS );

        matchedRules = rulesExecutor.processEvents( "{ \"ping\": { \"timeout\": true } }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.advanceTime( 3, TimeUnit.SECONDS );

        matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"storage\": { \"percent\":97 } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.advanceTime( 4, TimeUnit.SECONDS );

        matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"status\":\"stopped\" } } }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }
}
