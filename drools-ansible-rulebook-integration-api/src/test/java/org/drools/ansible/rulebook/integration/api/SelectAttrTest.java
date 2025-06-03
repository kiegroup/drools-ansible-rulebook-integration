package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SelectAttrTest {

    @Test
    void testSelectAttr() {

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
                                            "SelectAttrExpression": {
                                                "lhs": {
                                                    "Event": "people"
                                                },
                                                "rhs": {
                                                    "key": {
                                                        "String": "person.age"
                                                    },
                                                    "operator": {
                                                        "String": ">"
                                                    },
                                                    "value": {
                                                        "Integer": 30
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
                                                "message": "Has a person greater than 30"
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

        List<Match> matchedRules = rulesExecutor.processFacts( """
                { "people": [ { "person": { "name": "Fred", "age": 54 } }, 
                { "person": { "name": "Barney", "age": 45 } }, 
                { "person": { "name": "Wilma", "age": 23 } }, 
                { "person": { "name": "Betty", "age": 25 } } ] }
                """ ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( """
                { "people": [ { "person": { "name": "Wilma", "age": 23 } }, 
                { "person": { "name": "Betty", "age": 25 } } ] }
                """ ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testNegateSelectAttr() {

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
                                            "SelectAttrNotExpression": {
                                                "lhs": {
                                                    "Event": "people"
                                                },
                                                "rhs": {
                                                    "key": {
                                                        "String": "person.age"
                                                    },
                                                    "operator": {
                                                        "String": ">"
                                                    },
                                                    "value": {
                                                        "Integer": 30
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
                                                "message": "Has a person greater than 30"
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

        List<Match> matchedRules = rulesExecutor.processFacts( """
                { "people": [ { "person": { "name": "Fred", "age": 54 } }, 
                { "person": { "name": "Barney", "age": 45 } }, 
                { "person": { "name": "Wilma", "age": 23 } }, 
                { "person": { "name": "Betty", "age": 25 } } ] }
                """ ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( """
                { "people": [ { "person": { "name": "Wilma", "age": 23 } }, 
                { "person": { "name": "Betty", "age": 25 } } ] }
                """ ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( """
                { "people": [ { "person": { "name": "Wilma", "age": 43 } }, 
                { "person": { "name": "Betty", "age": 45 } } ] }
                """ ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testSelectAttrOnSingleItem() {

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
                                            "SelectAttrExpression": {
                                                "lhs": {
                                                    "Event": "person"
                                                },
                                                "rhs": {
                                                    "key": {
                                                        "String": "age"
                                                    },
                                                    "operator": {
                                                        "String": ">"
                                                    },
                                                    "value": {
                                                        "Integer": 30
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
                                                "message": "Has a person greater than 30"
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"person\": { \"name\": \"Fred\", \"age\": 54 } }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"person\": { \"name\": \"Wilma\", \"age\": 23 } }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testNegateSelectAttrOnSingleItem() {

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
                                            "SelectAttrNotExpression": {
                                                "lhs": {
                                                    "Event": "people"
                                                },
                                                "rhs": {
                                                    "key": {
                                                        "String": "person.age"
                                                    },
                                                    "operator": {
                                                        "String": ">"
                                                    },
                                                    "value": {
                                                        "Integer": 30
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
                                                "message": "Has a person greater than 30"
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"people\": { \"person\": { \"name\": \"Fred\", \"age\": 54 } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"people\": { \"person\": { \"name\": \"Wilma\", \"age\": 23 } } }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"people\": { \"man\": { \"name\": \"Barney\" } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testSelectAttrWithIn() {

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
                                            "SelectAttrExpression": {
                                                "lhs": {
                                                    "Event": "people"
                                                },
                                                "rhs": {
                                                    "key": {
                                                        "String": "person.age"
                                                    },
                                                    "operator": {
                                                        "String": "in"
                                                    },
                                                    "value": [\
                                                        {
                                                            "Integer": 25
                                                        },\
                                                        {
                                                            "Integer": 55
                                                        }\
                                                    ]
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
                                                "message": "Has a person greater than 30"
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

        List<Match> matchedRules = rulesExecutor.processFacts( """
                { "people": [ { "person": { "name": "Fred", "age": 54 } },
                { "person": { "name": "Barney", "age": 45 } },
                { "person": { "name": "Wilma", "age": 23 } },
                { "person": { "name": "Betty", "age": 25 } } ] }
                """ ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( """
                { "people": [ { "person": { "name": "Wilma", "age": 23 } },
                { "person": { "name": "Barney", "age": 45 } } ] }
                """ ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testSelectAttrNegated() {

        String JSON1 =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "Go",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SelectAttrNotExpression": {
                                                "lhs": {
                                                    "Event": "my_obj"
                                                },
                                                "rhs": {
                                                    "key": {
                                                        "String": "thing.size"
                                                    },
                                                    "operator": {
                                                        "String": ">="
                                                    },
                                                    "value": {
                                                        "Integer": 50
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

        List<Match> matchedRules = rulesExecutor.processFacts( """
                { "my_obj": [ { "thing": { "name": "a", "size": 51 } },
                { "thing": { "name": "b", "size": 31 } },
                { "thing": { "name": "c", "size": 89 } } ] }
                """ ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( """
                { "my_obj": [ { "thing": { "name": "a", "size": 51 } },
                { "thing": { "name": "b", "size": 61 } },
                { "thing": { "name": "c", "size": 89 } } ] }
                """ ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testSelectAttrIncompatibleTypes() {

        String JSON1 =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "Go",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SelectAttrExpression": {
                                                "lhs": {
                                                    "Event": "my_obj"
                                                },
                                                "rhs": {
                                                    "key": {
                                                        "String": "thing.size"
                                                    },
                                                    "operator": {
                                                        "String": ">="
                                                    },
                                                    "value": {
                                                        "Integer": 50
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

        List<Match> matchedRules = rulesExecutor.processFacts( """
                { "my_obj": [ { "thing": { "name": "a", "size": "large" } },
                { "thing": { "name": "b", "size": "medium" } },
                { "thing": { "name": "c", "size": "small" } } ] }
                """ ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    void testSelectAttrWithScientificNotation() {

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
                                            "SelectAttrExpression": {
                                                "lhs": {
                                                    "Event": "people"
                                                },
                                                "rhs": {
                                                    "key": {
                                                        "String": "person.age"
                                                    },
                                                    "operator": {
                                                        "String": "contains"
                                                    },
                                                    "value": {
                                                        "Integer": 1.021e+3
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
                                                "message": "Has a person greater than 30"
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

        List<Match> matchedRules = rulesExecutor.processFacts( """
                { "people": [ { "person": { "name": "Fred", "age": 54 } },
                { "person": { "name": "Barney", "age": 45 } },
                { "person": { "name": "Wilma", "age": 23 } },
                { "person": { "name": "Betty", "age": 1021 } } ] }
                """ ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( """
                { "people": [ { "person": { "name": "Wilma", "age": 23 } },
                { "person": { "name": "Betty", "age": 25 } } ] }
                """ ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }
}
