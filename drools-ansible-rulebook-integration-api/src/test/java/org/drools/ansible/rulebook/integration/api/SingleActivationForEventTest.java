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
                """
                {
                    "rules": [
                        {
                          "Rule": {
                            "name": "R1",
                            "condition": {
                              "AllCondition": [
                                {
                                  "SelectAttrExpression": {
                                    "lhs": {
                                      "Event": "planets"
                                    },
                                    "rhs": {
                                      "key": {
                                        "String": "planet.radius"
                                      },
                                      "operator": {
                                        "String": "<"
                                      },
                                      "value": {
                                        "Float": 1200.05
                                      }
                                    }
                                  }
                                }
                              ]
                            },
                            "actions": [
                              {
                                "Action": {
                                  "action": "debug",
                                  "action_args": {
                                    "msg": "Output for testcase #04"
                                  }
                                }
                              }
                            ],
                            "enabled": true
                          }
                        },
                        {
                          "Rule": {
                            "name": "R2",
                            "condition": {
                              "AllCondition": [
                                {
                                  "SelectAttrExpression": {
                                    "lhs": {
                                      "Event": "planets"
                                    },
                                    "rhs": {
                                      "key": {
                                        "String": "planet.radius"
                                      },
                                      "operator": {
                                        "String": ">="
                                      },
                                      "value": {
                                        "Float": 1188.3
                                      }
                                    }
                                  }
                                }
                              ]
                            },
                            "actions": [
                              {
                                "Action": {
                                  "action": "debug",
                                  "action_args": {
                                    "msg": "Output for testcase #05"
                                  }
                                }
                              }
                            ],
                            "enabled": true
                          }
                        }
                      ]
                }
                """;

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
