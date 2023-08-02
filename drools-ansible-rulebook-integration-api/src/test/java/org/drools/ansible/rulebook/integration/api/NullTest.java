package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class NullTest {

    @Test
    public void test() {
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
                                                            "Event": "x"
                                                        },
                                                        "rhs": {
                                                            "Integer": 1
                                                        }
                                                    }
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "y"
                                                        },
                                                        "rhs": {
                                                            "NullType": null
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
                                            "action": "print_event",
                                            "action_args": {
                                                "pretty": true
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"x\":1, \"y\":null }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"x\":1 }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testWithSelectAttr() {
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
                                                            "Event": "x"
                                                        },
                                                        "rhs": {
                                                            "Integer": 1
                                                        }
                                                    }
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "y"
                                                        },
                                                        "rhs": {
                                                            "NullType": null
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
                                            "action": "print_event",
                                            "action_args": {
                                                "pretty": true
                                            }
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
                                            "SelectAttrExpression": {
                                                "lhs": {
                                                    "Event": "persons"
                                                },
                                                "rhs": {
                                                    "key": {
                                                        "String": "occupation"
                                                    },
                                                    "operator": {
                                                        "String": "=="
                                                    },
                                                    "value": {
                                                        "NullType": null
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                },
                                "actions": [
                                    {
                                        "Action": {
                                            "action": "print_event",
                                            "action_args": {
                                                "pretty": true
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"x\":1, \"y\":null }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( """
                { "persons": [ { "age": 45, "name": "Fred", "occupation": "Dino Driver" },\
                { "age": 46, "name": "Barney", "occupation": null } ] }
                """ ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r2", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }
}
