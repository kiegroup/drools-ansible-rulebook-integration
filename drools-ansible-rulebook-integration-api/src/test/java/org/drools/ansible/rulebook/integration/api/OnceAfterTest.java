package org.drools.ansible.rulebook.integration.api;

import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeAmount;
import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.drools.ansible.rulebook.integration.protoextractor.ExtractorParser;
import org.drools.ansible.rulebook.integration.protoextractor.ExtractorUtils;
import org.junit.Test;
import org.kie.api.prototype.PrototypeFactInstance;
import org.kie.api.runtime.rule.Match;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OnceAfterTest {

    @Test
    public void testOnceAfterWithOr() {
        String json =
                """
                {\
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
                                    "once_after": "10 seconds"
                                }
                            }
                        }
                    ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.advanceTime(3, TimeUnit.SECONDS).join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h2\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"warning\" } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.advanceTime(4, TimeUnit.SECONDS).join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"error\" } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.advanceTime(5, TimeUnit.SECONDS).join();
        assertEquals(1, matchedRules.size());
        assertEquals("r1", matchedRules.get(0).getRule().getName());

        for (int i = 0; i < 3; i++) {
            PrototypeFactInstance fact = (PrototypeFactInstance) matchedRules.get(0).getDeclarationValue("m_" + i);
            String host = evalAgainstFact(fact, "meta.hosts").toString();
            assertTrue( host.equals( "h1" ) || host.equals( "h2" ) );
            String level = evalAgainstFact(fact, "alert.level").toString();
            assertTrue( level.equals( "error" ) || level.equals( "warning" ) );

            Map map = (Map) fact.asMap();
            Map ruleEngineMeta = (Map) ((Map)map.get(RulesModelUtil.META_FIELD)).get(RulesModelUtil.RULE_ENGINE_META_FIELD);
            assertEquals( new TimeAmount(10, TimeUnit.SECONDS).toString(), ruleEngineMeta.get("once_after_time_window") );
            assertEquals( i == 0 ? 2 : 1, ruleEngineMeta.get("events_in_window") );
        }

        for (int i = 0; i < 2; i++) {
            matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"h1\" }, \"alert\": { \"level\":\"warning\" } }").join();
            assertEquals(0, matchedRules.size());

            matchedRules = rulesExecutor.advanceTime(11, TimeUnit.SECONDS).join();
            assertEquals(1, matchedRules.size());
            assertEquals("r1", matchedRules.get(0).getRule().getName());

            PrototypeFactInstance fact = (PrototypeFactInstance) matchedRules.get(0).getDeclarationValue("m");
            String host = evalAgainstFact(fact, "meta.hosts").toString();
            assertTrue(host.equals("h1"));
            String level = evalAgainstFact(fact, "alert.level").toString();
            assertTrue(level.equals("warning"));

            Map map = (Map) fact.asMap();
            Map ruleEngineMeta = (Map) ((Map)map.get(RulesModelUtil.META_FIELD)).get(RulesModelUtil.RULE_ENGINE_META_FIELD);
            assertEquals( new TimeAmount(10, TimeUnit.SECONDS).toString(), ruleEngineMeta.get("once_after_time_window") );
            assertEquals( 1, ruleEngineMeta.get("events_in_window") );
        }

        rulesExecutor.dispose();
    }

    private static Object evalAgainstFact(PrototypeFactInstance fact, String expr) {
        return ExtractorUtils.getValueFrom(ExtractorParser.parse(expr), fact.asMap());
    }
        
    @Test
    public void test57_once_after_multiple() {
        final String RULES = """
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
                                                        "Event": "r1.level"
                                                    },
                                                    "rhs": {
                                                        "String": "warning"
                                                    }
                                                }
                                            },
                                            "rhs": {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "r1.level"
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
                            "actions": [
                                {
                                    "Action": {
                                        "action": "debug",
                                        "action_args": {
                                            "msg": "r1 works"
                                        }
                                    }
                                }
                            ],
                            "enabled": true,
                            "throttle": {
                                "group_by_attributes": [
                                    "event.meta.hosts",
                                    "event.r1.level"
                                ],
                                "once_after": "15 seconds"
                            }
                        }
                    },
                    {
                        "Rule": {
                            "name": "r2",
                            "condition": {
                                "AllCondition": [
                                    {
                                        "OrExpression": {
                                            "lhs": {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "r2.level"
                                                    },
                                                    "rhs": {
                                                        "String": "warning"
                                                    }
                                                }
                                            },
                                            "rhs": {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "r2.level"
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
                            "actions": [
                                {
                                    "Action": {
                                        "action": "debug",
                                        "action_args": {
                                            "msg": "r2 works"
                                        }
                                    }
                                }
                            ],
                            "enabled": true,
                            "throttle": {
                                "group_by_attributes": [
                                    "event.meta.hosts",
                                    "event.r2.level"
                                ],
                                "once_after": "30 seconds"
                            }
                        }
                    },
                    {
                        "Rule": {
                            "name": "r3",
                            "condition": {
                                "AllCondition": [
                                    {
                                        "OrExpression": {
                                            "lhs": {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "r3.level"
                                                    },
                                                    "rhs": {
                                                        "String": "warning"
                                                    }
                                                }
                                            },
                                            "rhs": {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "r3.level"
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
                            "actions": [
                                {
                                    "Action": {
                                        "action": "debug",
                                        "action_args": {
                                            "msg": "r3 works"
                                        }
                                    }
                                }
                            ],
                            "enabled": true,
                            "throttle": {
                                "group_by_attributes": [
                                    "event.meta.hosts",
                                    "event.r3.level"
                                ],
                                "once_after": "45 seconds"
                            }
                        }
                    }
                ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK), RULES);
        List<Match> matchedRules;

        source_generic_loop57(rulesExecutor);
        
        matchedRules = rulesExecutor.advanceTime(18, TimeUnit.SECONDS).join();
        assertThat(matchedRules)
            .hasSize(1)
            .map(m -> m.getRule().getName())
            .containsExactly("r1");

        source_generic_loop57(rulesExecutor);

        matchedRules = rulesExecutor.advanceTime(18, TimeUnit.SECONDS).join();
        assertThat(matchedRules)
            .hasSize(2)
            .map(m -> m.getRule().getName())
            .containsExactlyInAnyOrder("r2", "r1");
        
        source_generic_loop57(rulesExecutor);

        matchedRules = rulesExecutor.advanceTime(18, TimeUnit.SECONDS).join();
        assertThat(matchedRules)
            .hasSize(2)
            .map(m -> m.getRule().getName())
            .containsExactlyInAnyOrder("r3", "r1");

        rulesExecutor.dispose();
    }

    private void source_generic_loop57(RulesExecutor rulesExecutor) {
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"localhost.11\" }, \"r1\": { \"level\": \"warning\", \"message\": \"Low disk space\" } }").join();
        assertEquals(0, matchedRules.size());
        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"localhost.12\" }, \"r1\": { \"level\": \"warning\", \"message\": \"Low disk space\" } }").join();
        assertEquals(0, matchedRules.size());
        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"localhost.21\" }, \"r2\": { \"level\": \"warning\", \"message\": \"Low disk space\" } }").join();
        assertEquals(0, matchedRules.size());
        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"localhost.22\" }, \"r2\": { \"level\": \"warning\", \"message\": \"Low disk space\" } }").join();
        assertEquals(0, matchedRules.size());
        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"localhost.31\" }, \"r3\": { \"level\": \"warning\", \"message\": \"Low disk space\" } }").join();
        assertEquals(0, matchedRules.size());
        matchedRules = rulesExecutor.processEvents("{ \"meta\": { \"hosts\":\"localhost.32\" }, \"r3\": { \"level\": \"warning\", \"message\": \"Low disk space\" } }").join();
        assertEquals(0, matchedRules.size());
    }
}
