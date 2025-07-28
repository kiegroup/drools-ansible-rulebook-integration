package org.drools.ansible.rulebook.integration.api;

import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeAmount;
import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.junit.jupiter.api.Test;
import org.kie.api.prototype.PrototypeFactInstance;
import org.kie.api.runtime.rule.Match;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OnceWithinTest {

    @Test
    void testOnceWithinInCondition() {
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
                               ],
                               "throttle": {
                                   "group_by_attributes": [
                                       "event.sensu.host",
                                       "event.sensu.process.type"
                                   ],
                                   "once_within": "10 seconds"
                               }
                            },
                            "action":{
                               "assert_fact":{
                                  "ruleset":"Test rules4",
                                  "fact":{
                                     "j":1
                                  }
                               }
                            }
                         }
                      }
                   ]
                }
                """;

        onceWithinTest(json);
    }

    @Test
    void testOnceWithinInRule() {
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
                            "throttle":{
                               "group_by_attributes":[
                                  "event.sensu.host",
                                  "event.sensu.process.type"
                               ],
                               "once_within":"10 seconds"
                            }
                         }
                      }
                   ]
                }
                """;

        onceWithinTest(json);
    }

    private void onceWithinTest(String json) {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(1, matchedRules.size());

        PrototypeFactInstance fact = (PrototypeFactInstance) matchedRules.get(0).getDeclarationValue("singleton");
        Map map = (Map) fact.asMap();
        Map ruleEngineMeta = (Map) ((Map)map.get(RulesModelUtil.META_FIELD)).get(RulesModelUtil.RULE_ENGINE_META_FIELD);
        assertEquals( new TimeAmount(10, TimeUnit.SECONDS).toString(), ruleEngineMeta.get("once_within_time_window") );

        rulesExecutor.advanceTime(3, TimeUnit.SECONDS);

        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h2\" } }").join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.advanceTime(4, TimeUnit.SECONDS);

        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(0, matchedRules.size());

        rulesExecutor.advanceTime(5, TimeUnit.SECONDS);

        matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }").join();
        assertEquals(1, matchedRules.size());

        // verify SessionStats


        rulesExecutor.dispose();
    }

    @Test
    void testOnceWithinWithOr() {
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
                                    "once_within": "10 seconds"
                                }
                            }
                        }
                    ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.advanceTime(3, TimeUnit.SECONDS);

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"warning\" } }").join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.advanceTime(4, TimeUnit.SECONDS);

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        rulesExecutor.advanceTime(5, TimeUnit.SECONDS);

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    void testOnceWithinWithAnd() {
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
                                    "once_within": "10 seconds"
                                }
                            }
                        }
                    ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.advanceTime(3, TimeUnit.SECONDS);

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        rulesExecutor.advanceTime(8, TimeUnit.SECONDS);

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    void testRepeatedOnceWithin() {
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
                                    "once_within": "10 seconds"
                                }
                            }
                        }
                    ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        for (int i = 0; i < 3; i++) {
            processEvent(rulesExecutor, "{ \"meta\": { \"hosts\":\"HostA\" }, \"alert\": { \"level\":\"warning\" } }", 1);
            processEvent(rulesExecutor, "{ \"meta\": { \"hosts\":\"HostB\" }, \"alert\": { \"level\":\"error\" } }", 1);
            processEvent(rulesExecutor, "{ \"meta\": { \"hosts\":\"HostA\" }, \"alert\": { \"level\":\"warning\" } }", 0);
            processEvent(rulesExecutor, "{ \"meta\": { \"hosts\":\"HostB\" }, \"alert\": { \"level\":\"error\" } }", 0);
            processEvent(rulesExecutor, "{ \"meta\": { \"hosts\":\"HostA\" }, \"alert\": { \"level\":\"warning\" } }", 0);
            processEvent(rulesExecutor, "{ \"meta\": { \"hosts\":\"HostB\" }, \"alert\": { \"level\":\"error\" } }", 0);
            rulesExecutor.advanceTime(15, TimeUnit.SECONDS);
        }
    }

    private void processEvent(RulesExecutor rulesExecutor, String payload, int expectedFires) {
        List<Match> matchedRules = rulesExecutor.processEvents(payload).join();
        assertEquals(expectedFires, matchedRules.size());
    }
}
