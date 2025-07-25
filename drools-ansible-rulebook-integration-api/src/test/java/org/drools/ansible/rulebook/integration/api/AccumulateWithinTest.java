package org.drools.ansible.rulebook.integration.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeAmount;
import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.junit.jupiter.api.Test;
import org.kie.api.prototype.PrototypeFactInstance;
import org.kie.api.runtime.rule.Match;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AccumulateWithinTest {

    @Test
    void testAccumulateWithinThresholdMet() {
        String json =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "condition":{
                                       "AllCondition":[
                                          {
                                             "AssignmentExpression":{
                                                "lhs":{
                                                   "Events":"singleton"
                                                },
                                                "rhs":{
                                                   "EqualsExpression":{
                                                      "lhs":{
                                                         "Event":"sensu.process.type"
                                                      },
                                                      "rhs":{
                                                         "String":"alert"
                                                      }
                                                   }
                                                }
                                             }
                                          }
                                       ]
                                    },
                                    "action":{
                                       "assert_fact":{
                                          "ruleset":"Test rules4",
                                          "fact":{
                                             "j":1
                                          }
                                       }
                                    },
                                    "throttle": {
                                       "group_by_attributes": [
                                          "event.sensu.host",
                                          "event.sensu.process.type"
                                       ],
                                       "accumulate_within": "10 minutes",
                                       "threshold": 3
                                    }
                                 }
                              }
                           ]
                        }
                        """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // First event - no fire
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        // Second event - no fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        // Third event - threshold met, should fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(1, matchedRules.size());

        // Verify metadata
        PrototypeFactInstance fact = (PrototypeFactInstance) matchedRules.get(0).getDeclarationValue("singleton");
        Map map = (Map) fact.asMap();
        Map ruleEngineMeta = (Map) ((Map)map.get(RulesModelUtil.META_FIELD)).get(RulesModelUtil.RULE_ENGINE_META_FIELD);
        assertEquals(new TimeAmount(10, TimeUnit.MINUTES).toString(), ruleEngineMeta.get("accumulate_within_time_window"));
        assertEquals(3, ruleEngineMeta.get("threshold"));

        // Fourth event - starts new accumulation window, no fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    void testAccumulateWithinTimeout() {
        String json =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "condition":{
                                       "AllCondition":[
                                          {
                                             "AssignmentExpression":{
                                                "lhs":{
                                                   "Events":"singleton"
                                                },
                                                "rhs":{
                                                   "EqualsExpression":{
                                                      "lhs":{
                                                         "Event":"sensu.process.type"
                                                      },
                                                      "rhs":{
                                                         "String":"alert"
                                                      }
                                                   }
                                                }
                                             }
                                          }
                                       ]
                                    },
                                    "action":{
                                       "assert_fact":{
                                          "ruleset":"Test rules4",
                                          "fact":{
                                             "j":1
                                          }
                                       }
                                    },
                                    "throttle": {
                                       "group_by_attributes": [
                                          "event.sensu.host",
                                          "event.sensu.process.type"
                                       ],
                                       "accumulate_within": "10 seconds",
                                       "threshold": 3
                                    }
                                 }
                              }
                           ]
                        }
                        """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // First event - no fire
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        // Second event - no fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        // Advance time beyond window - should silently discard accumulated events
        rulesExecutor.advanceTime(11, TimeUnit.SECONDS);

        // New event after timeout - starts new accumulation, no fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        // Second event in new window - no fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        // Third event in new window - threshold met, should fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    void testAccumulateWithinDifferentGroups() {
        String json =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "condition":{
                                       "AllCondition":[
                                          {
                                             "AssignmentExpression":{
                                                "lhs":{
                                                   "Events":"singleton"
                                                },
                                                "rhs":{
                                                   "EqualsExpression":{
                                                      "lhs":{
                                                         "Event":"sensu.process.type"
                                                      },
                                                      "rhs":{
                                                         "String":"alert"
                                                      }
                                                   }
                                                }
                                             }
                                          }
                                       ]
                                    },
                                    "action":{
                                       "assert_fact":{
                                          "ruleset":"Test rules4",
                                          "fact":{
                                             "j":1
                                          }
                                       }
                                    },
                                    "throttle": {
                                       "group_by_attributes": [
                                          "event.sensu.host",
                                          "event.sensu.process.type"
                                       ],
                                       "accumulate_within": "10 minutes",
                                       "threshold": 2
                                    }
                                 }
                              }
                           ]
                        }
                        """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // First event for h1 - no fire
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        // First event for h2 - no fire (different group)
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h2\" } }").join();
        assertEquals(0, matchedRules.size());

        // Second event for h1 - threshold met for h1, should fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(1, matchedRules.size());

        // Second event for h2 - threshold met for h2, should fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h2\" } }").join();
        assertEquals(1, matchedRules.size());

        // Third event for h1 - starts new accumulation for h1, no fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    void testAccumulateWithinMixedEvents() {
        String json =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "condition":{
                                       "AllCondition":[
                                          {
                                             "AssignmentExpression":{
                                                "lhs":{
                                                   "Events":"singleton"
                                                },
                                                "rhs":{
                                                   "EqualsExpression":{
                                                      "lhs":{
                                                         "Event":"sensu.process.type"
                                                      },
                                                      "rhs":{
                                                         "String":"alert"
                                                      }
                                                   }
                                                }
                                             }
                                          }
                                       ]
                                    },
                                    "action":{
                                       "assert_fact":{
                                          "ruleset":"Test rules4",
                                          "fact":{
                                             "j":1
                                          }
                                       }
                                    },
                                    "throttle": {
                                       "group_by_attributes": [
                                          "event.sensu.host",
                                          "event.sensu.process.type"
                                       ],
                                       "accumulate_within": "10 seconds",
                                       "threshold": 3
                                    }
                                 }
                              }
                           ]
                        }
                        """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // Event 1 for h1 - no fire
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        rulesExecutor.advanceTime(3, TimeUnit.SECONDS);

        // Event 2 for h1 - no fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        // Event for h2 - different group, no fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h2\" } }").join();
        assertEquals(0, matchedRules.size());

        rulesExecutor.advanceTime(3, TimeUnit.SECONDS);

        // Event 3 for h1 - threshold met, should fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(1, matchedRules.size());

        // Event for h2 - still only 1 event for h2, no fire
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h2\" } }").join();
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    void testAccumulateWithinWithOr() {
        String json =
                """
                        {
                            "rules": [
                                {
                                    "Rule": {
                                        "name": "r1",
                                        "condition": {
                                            "AllCondition": [
                                                {
                                                    "OrExpression": {
                                                        "lhs": {
                                                            "EqualsExpression": {
                                                                "lhs": {
                                                                    "Event": "alert.level"
                                                                },
                                                                "rhs": {
                                                                    "String": "warning"
                                                                }
                                                            }
                                                        },
                                                        "rhs": {
                                                            "EqualsExpression": {
                                                                "lhs": {
                                                                    "Event": "alert.level"
                                                                },
                                                                "rhs": {
                                                                    "String": "error"
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                        "action": {
                                            "Action": {
                                                "action": "print_event",
                                                "action_args": {}
                                            }
                                        },
                                        "enabled": true,
                                        "throttle": {
                                            "group_by_attributes": [
                                                "event.meta.hosts",
                                                "event.alert.level"
                                            ],
                                            "accumulate_within": "10 seconds",
                                            "threshold": 2
                                        }
                                    }
                                }
                            ]
                        }
                        """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // First error event for h1 - no fire
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        // Second error event for h1 - threshold met, should fire
        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(1, matchedRules.size());

        // First warning event for h1 - different group (different level), no fire
        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"warning\" } }").join();
        assertEquals(0, matchedRules.size());

        // Second warning event for h1 - threshold met for warning group, should fire
        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"warning\" } }").join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    void testAccumulateWithinWithAnd() {
        String json =
                """
                        {
                            "rules": [
                                {
                                    "Rule": {
                                        "name": "r1",
                                        "condition": {
                                            "AllCondition": [
                                                {
                                                    "AndExpression": {
                                                        "lhs": {
                                                            "EqualsExpression": {
                                                                "lhs": {
                                                                    "Event": "alert.level"
                                                                },
                                                                "rhs": {
                                                                    "String": "error"
                                                                }
                                                            }
                                                        },
                                                        "rhs": {
                                                            "EqualsExpression": {
                                                                "lhs": {
                                                                    "Event": "meta.hosts"
                                                                },
                                                                "rhs": {
                                                                    "String": "h1"
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                        "action": {
                                            "Action": {
                                                "action": "print_event",
                                                "action_args": {}
                                            }
                                        },
                                        "enabled": true,
                                        "throttle": {
                                            "group_by_attributes": [
                                                "event.meta.hosts",
                                                "event.alert.level"
                                            ],
                                            "accumulate_within": "10 seconds",
                                            "threshold": 2
                                        }
                                    }
                                }
                            ]
                        }
                        """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // First matching event - no fire
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        // Non-matching event (wrong host) - should not count
        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h2\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        // Second matching event - threshold met, should fire
        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    void testRepeatedAccumulateWithin() {
        String json =
                """
                        {
                            "rules": [
                                {
                                    "Rule": {
                                        "name": "r1",
                                        "condition": {
                                            "AllCondition": [
                                                {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "alert.level"
                                                        },
                                                        "rhs": {
                                                            "String": "error"
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                        "action": {
                                            "Action": {
                                                "action": "print_event",
                                                "action_args": {}
                                            }
                                        },
                                        "enabled": true,
                                        "throttle": {
                                            "group_by_attributes": [
                                                "event.meta.hosts"
                                            ],
                                            "accumulate_within": "10 seconds",
                                            "threshold": 2
                                        }
                                    }
                                }
                            ]
                        }
                        """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // Test multiple cycles to ensure consistent behavior
        for (int i = 0; i < 3; i++) {
            // First event - no fire
            processEvent(rulesExecutor, "{ \"meta\": { \"hosts\":\"HostA\" }, \"alert\": { \"level\":\"error\" } }", 0);
            
            // Second event - threshold met, should fire
            processEvent(rulesExecutor, "{ \"meta\": { \"hosts\":\"HostA\" }, \"alert\": { \"level\":\"error\" } }", 1);
            
            // Third event - new cycle, no fire
            processEvent(rulesExecutor, "{ \"meta\": { \"hosts\":\"HostA\" }, \"alert\": { \"level\":\"error\" } }", 0);
            
            // Let window expire
            rulesExecutor.advanceTime(11, TimeUnit.SECONDS);
            
            // New cycle after timeout
            processEvent(rulesExecutor, "{ \"meta\": { \"hosts\":\"HostA\" }, \"alert\": { \"level\":\"error\" } }", 0);
            processEvent(rulesExecutor, "{ \"meta\": { \"hosts\":\"HostA\" }, \"alert\": { \"level\":\"error\" } }", 1);
        }

        rulesExecutor.dispose();
    }

    @Test
    void testAccumulateWithinThresholdOne() {
        String json =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "condition":{
                                       "AllCondition":[
                                          {
                                             "AssignmentExpression":{
                                                "lhs":{
                                                   "Events":"singleton"
                                                },
                                                "rhs":{
                                                   "EqualsExpression":{
                                                      "lhs":{
                                                         "Event":"sensu.process.type"
                                                      },
                                                      "rhs":{
                                                         "String":"alert"
                                                      }
                                                   }
                                                }
                                             }
                                          }
                                       ]
                                    },
                                    "action":{
                                       "assert_fact":{
                                          "ruleset":"Test rules4",
                                          "fact":{
                                             "j":1
                                          }
                                       }
                                    },
                                    "throttle": {
                                       "group_by_attributes": [
                                          "event.sensu.host"
                                       ],
                                       "accumulate_within": "10 seconds",
                                       "threshold": 1
                                    }
                                 }
                              }
                           ]
                        }
                        """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // First event with threshold 1 - should fire immediately
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(1, matchedRules.size());

        // Second event - new cycle, should fire immediately again
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    void testAccumulateWithinExactWindowExpiration() {
        String json =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "condition":{
                                       "AllCondition":[
                                          {
                                             "AssignmentExpression":{
                                                "lhs":{
                                                   "Events":"singleton"
                                                },
                                                "rhs":{
                                                   "EqualsExpression":{
                                                      "lhs":{
                                                         "Event":"sensu.process.type"
                                                      },
                                                      "rhs":{
                                                         "String":"alert"
                                                      }
                                                   }
                                                }
                                             }
                                          }
                                       ]
                                    },
                                    "action":{
                                       "assert_fact":{
                                          "ruleset":"Test rules4",
                                          "fact":{
                                             "j":1
                                          }
                                       }
                                    },
                                    "throttle": {
                                       "group_by_attributes": [
                                          "event.sensu.host"
                                       ],
                                       "accumulate_within": "10 seconds",
                                       "threshold": 3
                                    }
                                 }
                              }
                           ]
                        }
                        """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // First event
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        // Second event
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        // Advance to exactly window expiration
        rulesExecutor.advanceTime(10, TimeUnit.SECONDS);

        // Event at exact expiration - should start new window
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        // Continue new window
        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.dispose();
    }

    private void processEvent(RulesExecutor rulesExecutor, String payload, int expectedFires) {
        List<Match> matchedRules = rulesExecutor.processEvents(payload).join();
        assertEquals(expectedFires, matchedRules.size());
    }
}