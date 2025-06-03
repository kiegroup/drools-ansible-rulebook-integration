package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ListContainsTest {

    @Test
    void testListContainsInt() {

        String JSON1 =
                """
                {
                    "rules": [
                            {
                                    "Rule": {
                                        "name": "contains_rule_int",
                                        "condition": {
                                            "AllCondition": [
                                                {
                                                    "ListContainsItemExpression": {
                                                        "lhs": {
                                                            "Event": "id_list"
                                                        },
                                                        "rhs": {
                                                            "Integer": 1
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                        "action": {
                                            "Action": {
                                                "action": "debug",
                                                "action_args": {}
                                            }
                                        },
                                        "enabled": true
                                    }
                                }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [2,4] }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [1,3,5] }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : 2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : 1 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    void testListNotContainsString() {

        String JSON1 =
                """
                {
                    "rules": [
                            {
                                    "Rule": {
                                        "name": "contains_rule_int",
                                        "condition": {
                                            "AllCondition": [
                                                {
                                                    "ListNotContainsItemExpression": {
                                                        "lhs": {
                                                            "Event": "friends"
                                                        },
                                                        "rhs": {
                                                            "String": "pebbles"
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                        "action": {
                                            "Action": {
                                                "action": "debug",
                                                "action_args": {}
                                            }
                                        },
                                        "enabled": true
                                    }
                                }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"friends\" : [\"fred\", \"pebbles\"] }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"friends\" : [\"fred\", \"barney\"] }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    void testIntInList() {

        String JSON1 =
                """
                {
                    "rules": [
                            {
                                    "Rule": {
                                        "name": "in_rule_int",
                                        "condition": {
                                            "AllCondition": [
                                                {
                                                    "ItemInListExpression": {
                                                        "lhs": {
                                                            "Event": "i"
                                                        },
                                                        "rhs": [
                                                            {
                                                                "Integer": 1
                                                            },
                                                            {
                                                                "Integer": 2
                                                            },
                                                            {
                                                                "Integer": 3
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        },
                                        "action": {
                                            "Action": {
                                                "action": "debug",
                                                "action_args": {}
                                            }
                                        },
                                        "enabled": true
                                    }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\" : 4 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\" : 3 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "in_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    void testIntNotInList() {

        String JSON1 =
                """
                {
                    "rules": [
                            {
                                    "Rule": {
                                        "name": "not_in_rule_int",
                                        "condition": {
                                            "AllCondition": [
                                                {
                                                    "ItemNotInListExpression": {
                                                        "lhs": {
                                                            "Event": "i"
                                                        },
                                                        "rhs": [
                                                            {
                                                                "Integer": 1
                                                            },
                                                            {
                                                                "Integer": 2
                                                            },
                                                            {
                                                                "Integer": 3
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        },
                                        "action": {
                                            "Action": {
                                                "action": "debug",
                                                "action_args": {}
                                            }
                                        },
                                        "enabled": true
                                    }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\" : 3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\" : 4 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "not_in_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    void testListContainsWithScientificNotation() {

        String JSON1 =
                """
                {
                    "rules": [
                            {
                                    "Rule": {
                                        "name": "contains_rule_int",
                                        "condition": {
                                            "AllCondition": [
                                                {
                                                    "ListContainsItemExpression": {
                                                        "lhs": {
                                                            "Event": "id_list"
                                                        },
                                                        "rhs": {
                                                            "Integer": 1.021e+3
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                        "action": {
                                            "Action": {
                                                "action": "debug",
                                                "action_args": {}
                                            }
                                        },
                                        "enabled": true
                                    }
                                }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [2,4] }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [3,1021] }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"id_list\" : [3,1021.00] }" ).join(); // 1021.00 becomes BigDecimal("1021.00") by RulesModelUtil.asFactMap()
        assertEquals( 1, matchedRules.size() );
        assertEquals( "contains_rule_int", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    void testListContainsWithJoinCondition() {

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
                                            "ListContainsItemExpression": {
                                                "lhs": {
                                                    "Event": "thirdlist.rnames"
                                                },
                                                "rhs": {
                                                    "Events": "m_0.otherlist.resource_name"
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
