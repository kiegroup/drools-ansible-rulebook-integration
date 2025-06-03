package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SelectTest {

    @Test
    void testSelect() {

        String JSON1 =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "r1",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SelectExpression": {
                                                "lhs": {
                                                    "Event": "levels"
                                                },
                                                "rhs": {
                                                    "operator": {
                                                        "String": ">"
                                                    },
                                                    "value": {
                                                        "Integer": 25
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                },
                                "actions": [
                                    {
                                        "Action": {
                                            "action": "echo",
                                            "action_args": {
                                                "message": "Hurray"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Fred\", \"age\": 54, \"levels\": [ 10, 20, 30] }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Barney\", \"age\": 53, \"levels\": [ 11, 15, 16] }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testSelectOnSingleItem() {

        String JSON1 =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "r1",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SelectExpression": {
                                                "lhs": {
                                                    "Event": "levels"
                                                },
                                                "rhs": {
                                                    "operator": {
                                                        "String": ">"
                                                    },
                                                    "value": {
                                                        "Integer": 25
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                },
                                "actions": [
                                    {
                                        "Action": {
                                            "action": "echo",
                                            "action_args": {
                                                "message": "Hurray"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Fred\", \"age\": 54, \"levels\": 30 }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Barney\", \"age\": 53, \"levels\": 16 }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testNegatedSelect() {

        String JSON1 =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "r1",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SelectNotExpression": {
                                                "lhs": {
                                                    "Event": "levels"
                                                },
                                                "rhs": {
                                                    "operator": {
                                                        "String": ">"
                                                    },
                                                    "value": {
                                                        "Integer": 25
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                },
                                "actions": [
                                    {
                                        "Action": {
                                            "action": "echo",
                                            "action_args": {
                                                "message": "Hurray"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Fred\", \"age\": 54, \"levels\": [ 10, 20, 30 ] }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Barney\", \"age\": 53, \"levels\": [ 11, 15, 16 ] }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Barney\", \"age\": 53, \"levels\": [ 31, 35, 36 ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testSelectWithRegEx() {

        String JSON1 =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "r1",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SelectExpression": {
                                                "lhs": {
                                                    "Event": "addresses"
                                                },
                                                "rhs": {
                                                    "operator": {
                                                        "String": "regex"
                                                    },
                                                    "value": {
                                                        "String": "Main St"
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                },
                                "actions": [
                                    {
                                        "Action": {
                                            "action": "echo",
                                            "action_args": {
                                                "message": "Hurray"
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
                                            "SelectNotExpression": {
                                                "lhs": {
                                                    "Event": "addresses"
                                                },
                                                "rhs": {
                                                    "operator": {
                                                       "String": "regex"
                                                     },
                                                     "value": {
                                                        "String": "Major St"
                                                     }
                                                  }
                                              }
                                          }
                                    ]
                                },
                                "actions": [
                                     {
                                         "Action": {
                                             "action": "echo",
                                             "action_args": {
                                                 "message": "No one lives on Major St"
                                             }
                                         }
                                      }
                                ],
                                "enabled": true
                             }
                         }\
                    ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Fred\", \"age\": 54, \"addresses\": [ \"123 Main St, Bedrock, MI\", \"545 Spring St, Cresskill, NJ\", \"435 Wall Street, New York, NY\"] }" ).join();
        assertEquals( 2, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"name\": \"Barney\", \"age\": 53, \"addresses\": [ \"432 Raymond Blvd, Newark, NJ\", \"145 Wall St, Dumont, NJ\"] }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testSelectWithFloat() {
        String JSON1 =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "test float",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SelectExpression": {
                                                "lhs": {
                                                    "Event": "radius"
                                                },
                                                "rhs": {
                                                    "operator": {
                                                        "String": ">="
                                                    },
                                                    "value": {
                                                        "Integer": 500
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
                                                "msg": "Float test passes"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"radius\": 600.0 }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"radius\": 400.0 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"radius\": 500.0 }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testSelectOnNull() {
        String JSON1 =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "with select",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SelectExpression": {
                                                "lhs": {
                                                    "Event": "my_obj"
                                                },
                                                "rhs": {
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"my_obj\": null }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"another_obj\": null }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testSelectOnField() {
        String JSON1 =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "R1",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SelectExpression": {
                                                "lhs": {
                                                    "Event": "my_list1"
                                                },
                                                "rhs": {
                                                    "operator": {
                                                        "String": "=="
                                                    },
                                                    "value": {
                                                        "Event": "my_int1"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"action\": \"go\", \"my_int1\": 3, \"my_list1\": [ 1, 3, 7 ] }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"action\": \"go\", \"my_int1\": 4, \"my_list1\": [ 1, 3, 7 ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }


    @Test
    void testSelectWithJoinCondition() {

        String JSON1 =
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
                                                    "Event": "otherlist.name"
                                                },
                                                "rhs": {
                                                    "String": "Delete"
                                                }
                                            }
                                        },
                                        {
                                            "SelectExpression": {
                                                "lhs": {
                                                    "Event": "thirdlist.rnames"
                                                },
                                                "rhs": {
                                                    "operator": {
                                                        "String": "search"
                                                    },
                                                    "value": {
                                                        "Events": "m_0.otherlist.resource_name"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"otherlist\": { \"name\": \"Delete\", \"resource_name\": \"fred\" } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"thirdlist\": { \"rnames\": [ \"fred\", \"barney\" ] } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }
}
