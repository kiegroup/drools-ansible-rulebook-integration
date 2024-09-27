package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class TypeMismatchTest {

    public static final String JSON1 =
            """
                {
                    "name": "ruleSet1",
                    "rules": [
                         {
                             "Rule": {
                                 "name": "r1",
                                 "condition": {
                                     "AllCondition": [
                                         {
                                             "EqualsExpression": {
                                                 "lhs": {
                                                     "Event": "meta.headers"
                                                 },
                                                 "rhs": {
                                                     "String": "Hello Testing World"
                                                 }
                                             }
                                         }
                                     ]
                                 },
                                 "actions": [
                                     {
                                         "Action": {
                                             "action": "debug",
                                             "action_args": {}
                                         }
                                     }
                                 ],
                                 "enabled": true
                             }
                         },
                         {
                             "Rule": {
                                 "name": "r2",
                                 "condition": {
                                     "AllCondition": [
                                         {
                                             "NotEqualsExpression": {
                                                 "lhs": {
                                                     "Event": "meta.headers"
                                                 },
                                                 "rhs": {
                                                     "String": "Hello Testing World"
                                                 }
                                             }
                                         }
                                     ]
                                 },
                                 "actions": [
                                     {
                                         "Action": {
                                             "action": "debug",
                                             "action_args": {}
                                         }
                                     }
                                 ],
                                 "enabled": true
                             }
                         }
                     ]
                }
                """;

    @Test
    public void mapAndString() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        // mera.headers is a map, not a string
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"meta\": {\"headers\": {\"Content-Length\": \"36\"}} } }").join();
        // When comparing mismatched types, the rule should not match (even r2). Logs error for 2 rules.
        // TODO: Add log assertion
        assertEquals(0, matchedRules.size());

        // One more time
        matchedRules = rulesExecutor.processEvents("{ \"meta\": {\"headers\": {\"Content-Length\": \"25\"}} } }").join();
        // not firing. Don't log errors again.
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    public static final String JSON2 =
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
                                                         "Event": "i"
                                                     },
                                                     "rhs": {
                                                         "Integer": 1
                                                     }
                                                 }
                                             }
                                         ]
                                     },
                                     "actions": [
                                         {
                                             "Action": {
                                                 "action": "debug",
                                                 "action_args": {}
                                             }
                                         }
                                     ],
                                     "enabled": true
                                 }
                             }
                         ]
                    }
                    """;

    @Test
    public void stringAndInteger() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON2);

        // mera.headers is a map, not a string
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": \"1\" }").join();

        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }


    @Test
    public void typeMismatchWithNodeSharing() {
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
                                                            "Event": "i"
                                                        },
                                                        "rhs": {
                                                            "Integer": 1
                                                        }
                                                    }
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "j"
                                                        },
                                                        "rhs": {
                                                            "Integer": 1
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
                                            "action_args": {}
                                        }
                                    }
                                ],
                                "enabled": true
                            }
                        },
                        {
                            "Rule": {
                                "name": "r2",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "AndExpression": {
                                                "lhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "i"
                                                        },
                                                        "rhs": {
                                                            "Integer": 1
                                                        }
                                                    }
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "j"
                                                        },
                                                        "rhs": {
                                                            "Integer": 2
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
                                            "action_args": {}
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

        // TODO: add node sharing assertion

        // i is a string, not an integer
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": \"1\", \"j\": 1 }").join();
        assertEquals(0, matchedRules.size());
    }
}