package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class StringMatchTest {

    @Test
    public void testMatch() {
        String json =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "name":"R1",
                            "condition":{
                               "AllCondition":[
                                  {
                                     "SearchMatchesExpression":{
                                        "lhs":{
                                           "Event":"url1"
                                        },
                                        "rhs": {
                                            "SearchType": {
                                                "kind": {
                                                    "String": "match"
                                                },
                                                "pattern": {
                                                    "String": "https://example.com/users/.*/resources"
                                                },
                                                "options": [
                                                    {
                                                        "name": {
                                                            "String": "ignorecase"
                                                        },
                                                        "value": {
                                                            "Boolean": true
                                                        }
                                                    },
                                                    {
                                                        "name": {
                                                            "String": "multiline"
                                                        },
                                                        "value": {
                                                            "Boolean": true
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                     }
                                  }
                               ]
                            }
                         }
                      }
                   ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.org/users/foo/resources/bar\" }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.com/users/foo/resources/bar\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.COM/users/foo/resources/bar\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testNotMatch() {
        String json =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "name":"R1",
                            "condition":{
                               "AllCondition":[
                                  {
                                     "SearchNotMatchesExpression":{
                                        "lhs":{
                                           "Event":"url1"
                                        },
                                        "rhs": {
                                            "SearchType": {
                                                "kind": {
                                                    "String": "match"
                                                },
                                                "pattern": {
                                                    "String": "https://example.com/users/.*/resources"
                                                },
                                                "options": [
                                                    {
                                                        "name": {
                                                            "String": "ignorecase"
                                                        },
                                                        "value": {
                                                            "Boolean": true
                                                        }
                                                    },
                                                    {
                                                        "name": {
                                                            "String": "multiline"
                                                        },
                                                        "value": {
                                                            "Boolean": true
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                     }
                                  }
                               ]
                            }
                         }
                      }
                   ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.org/users/foo/resources/bar\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.com/users/foo/resources/bar\" }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"url1\": \"https://example.COM/users/foo/resources/bar\" }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testMultiline() {
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
                                            "SearchMatchesExpression": {
                                                "lhs": {
                                                    "Event": "string1"
                                                },
                                                "rhs": {
                                                    "SearchType": {
                                                        "kind": {
                                                            "String": "search"
                                                        },
                                                        "pattern": {
                                                            "String": "multiline string"
                                                        },
                                                        "options": [
                                                            {
                                                                "name": {
                                                                    "String": "ignorecase"
                                                                },
                                                                "value": {
                                                                    "Boolean": false
                                                                }
                                                            },
                                                            {
                                                                "name": {
                                                                    "String": "multiline"
                                                                },
                                                                "value": {
                                                                    "Boolean": true
                                                                }
                                                            }
                                                        ]
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
                        },
                        {
                            "Rule": {
                                "name": "R2",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SearchMatchesExpression": {
                                                "lhs": {
                                                    "Event": "string2"
                                                },
                                                "rhs": {
                                                    "SearchType": {
                                                        "kind": {
                                                            "String": "match"
                                                        },
                                                        "pattern": {
                                                            "String": "This is a"
                                                        },
                                                        "options": [
                                                            {
                                                                "name": {
                                                                    "String": "ignorecase"
                                                                },
                                                                "value": {
                                                                    "Boolean": false
                                                                }
                                                            },
                                                            {
                                                                "name": {
                                                                    "String": "multiline"
                                                                },
                                                                "value": {
                                                                    "Boolean": true
                                                                }
                                                            }
                                                        ]
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
                        },
                        {
                            "Rule": {
                                "name": "R3",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SearchMatchesExpression": {
                                                "lhs": {
                                                    "Event": "string3"
                                                },
                                                "rhs": {
                                                    "SearchType": {
                                                        "kind": {
                                                            "String": "match"
                                                        },
                                                        "pattern": {
                                                            "String": "his is a"
                                                        },
                                                        "options": [
                                                            {
                                                                "name": {
                                                                    "String": "ignorecase"
                                                                },
                                                                "value": {
                                                                    "Boolean": false
                                                                }
                                                            },
                                                            {
                                                                "name": {
                                                                    "String": "multiline"
                                                                },
                                                                "value": {
                                                                    "Boolean": true
                                                                }
                                                            }
                                                        ]
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
                        },
                        {
                            "Rule": {
                                "name": "R4",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "SearchMatchesExpression": {
                                                "lhs": {
                                                    "Event": "string4"
                                                },
                                                "rhs": {
                                                    "SearchType": {
                                                        "kind": {
                                                            "String": "regex"
                                                        },
                                                        "pattern": {
                                                            "String": "^This.is.*"
                                                        },
                                                        "options": [
                                                            {
                                                                "name": {
                                                                    "String": "ignorecase"
                                                                },
                                                                "value": {
                                                                    "Boolean": false
                                                                }
                                                            },
                                                            {
                                                                "name": {
                                                                    "String": "multiline"
                                                                },
                                                                "value": {
                                                                    "Boolean": true
                                                                }
                                                            }
                                                        ]
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules;
        matchedRules = rulesExecutor.processFacts( "{ \"string1\": \"This is a\\nmultiline string for\\nthe purposes of testing search\\n\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"string2\": \"This is a\\nmultiline string for\\nthe purposes of testing search\\n\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R2", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"string2\": \"For the purposes of testing search\\nThis is a\\nmultiline string\\n\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R2", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"string3\": \"This is a\\nmultiline string for\\nthe purposes of testing search\\n\" }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"string4\": \"This is a\\nmultiline string for\\nthe purposes of testing search\\n\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R4", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSearchPatternInEvent() {
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
                                            "SearchMatchesExpression": {
                                                "lhs": {
                                                    "String": "select selectattr search matches should matches in"
                                                },
                                                "rhs": {
                                                    "SearchType": {
                                                        "kind": {
                                                            "String": "search"
                                                        },
                                                        "pattern": {
                                                            "Event": "my_str"
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

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"my_str\": \"must\" }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"my_str\": \"should\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testMatchPatternInEvent() {
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
                                            "SearchMatchesExpression": {
                                                "lhs": {
                                                    "String": "select selectattr search matches should matches in"
                                                },
                                                "rhs": {
                                                    "SearchType": {
                                                        "kind": {
                                                            "String": "match"
                                                        },
                                                        "pattern": {
                                                            "Event": "my_str"
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

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"my_str\": \"should\" }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"my_str\": \"select\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }
}
