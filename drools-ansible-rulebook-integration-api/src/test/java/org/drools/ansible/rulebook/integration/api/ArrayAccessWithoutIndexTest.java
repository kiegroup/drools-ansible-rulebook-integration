package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class ArrayAccessWithoutIndexTest {

    @Test
    public void testSelectAttrForArrayInArray() {

        String JSON_ARRAY_IN_ARRAY =
                """
                {
                  "rules":[
                    {
                      "Rule":{
                        "name":"r1",
                        "condition":{
                          "AllCondition":[
                            {
                              "SelectAttrExpression":{
                                "lhs":{
                                  "Event":"incident.alerts.tags"
                                },
                                "rhs":{
                                  "key":{
                                    "String":"value"
                                  },
                                  "operator":{
                                    "String":"=="
                                  },
                                  "value":{
                                    "String":"DiskUsage"
                                  }
                                }
                              }
                            }
                          ]
                        },
                        "actions":[
                          {
                            "Action":{
                              "action":"debug",
                              "action_args":{
                                "msg":"Found a match with alerts"
                              }
                            }
                          }
                        ],
                        "enabled":true
                      }
                    }
                  ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_ARRAY_IN_ARRAY);

        List<Match> matchedRules = rulesExecutor.processFacts( """
                {
                  "incident":{
                    "id":"aaa",
                    "active":false,
                    "alerts":[
                      {
                        "id":"bbb",
                        "tags":[
                          {
                            "name":"alertname",
                            "value":"MariadbDown"
                          },
                          {
                            "name":"severity",
                            "value":"critical"
                          }
                        ],
                        "status":"Ok"
                      },
                      {
                        "id":"ccc",
                        "tags":[
                          {
                            "name":"severity",
                            "value":"critical"
                          },
                          {
                            "name":"alertname",
                            "value":"DiskUsage"
                          }
                        ],
                        "status":"Ok"
                      }
                    ]
                  }
                }
                """ ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSelectAttrForArrayInMapInArray() {

        String JSON_ARRAY_IN_MAP_IN_ARRAY =
                """
                {
                  "rules":[
                    {
                      "Rule":{
                        "name":"r1",
                        "condition":{
                          "AllCondition":[
                            {
                              "SelectAttrExpression":{
                                "lhs":{
                                  "Event":"incident.alerts.meta.tags"
                                },
                                "rhs":{
                                  "key":{
                                    "String":"value"
                                  },
                                  "operator":{
                                    "String":"=="
                                  },
                                  "value":{
                                    "String":"DiskUsage"
                                  }
                                }
                              }
                            }
                          ]
                        },
                        "actions":[
                          {
                            "Action":{
                              "action":"debug",
                              "action_args":{
                                "msg":"Found a match with alerts"
                              }
                            }
                          }
                        ],
                        "enabled":true
                      }
                    }
                  ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_ARRAY_IN_MAP_IN_ARRAY);

        List<Match> matchedRules = rulesExecutor.processFacts( """
                {
                  "incident":{
                    "id":"aaa",
                    "active":false,
                    "alerts":[
                      {
                        "id":"bbb",
                        "meta": {
                          "tags":[
                            {
                              "name":"alertname",
                              "value":"MariadbDown"
                            },
                            {
                              "name":"severity",
                              "value":"critical"
                            }
                          ],
                          "status":"Ok"
                        }
                      },
                      {
                        "id":"ccc",
                        "meta": {
                          "tags":[
                            {
                              "name":"severity",
                              "value":"critical"
                            },
                            {
                              "name":"alertname",
                              "value":"DiskUsage"
                            }
                          ],
                          "status":"Ok"
                        }
                      }
                    ]
                  }
                }
                """ ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSelectAttrForArrayInArrayWithIndex() {

        String JSON_ARRAY_IN_ARRAY_WITH_INDEX =
                """
                {
                  "rules":[
                    {
                      "Rule":{
                        "name":"r1",
                        "condition":{
                          "AllCondition":[
                            {
                              "SelectAttrExpression":{
                                "lhs":{
                                  "Event":"incident.alerts.tags[1]"
                                },
                                "rhs":{
                                  "key":{
                                    "String":"value"
                                  },
                                  "operator":{
                                    "String":"=="
                                  },
                                  "value":{
                                    "String":"DiskUsage"
                                  }
                                }
                              }
                            }
                          ]
                        },
                        "actions":[
                          {
                            "Action":{
                              "action":"debug",
                              "action_args":{
                                "msg":"Found a match with alerts"
                              }
                            }
                          }
                        ],
                        "enabled":true
                      }
                    }
                  ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_ARRAY_IN_ARRAY_WITH_INDEX);

        List<Match> matchedRules = rulesExecutor.processFacts( """
                {
                  "incident":{
                    "id":"aaa",
                    "active":false,
                    "alerts":[
                      {
                        "id":"bbb",
                        "tags":[
                          {
                            "name":"alertname",
                            "value":"MariadbDown"
                          },
                          {
                            "name":"severity",
                            "value":"critical"
                          }
                        ],
                        "status":"Ok"
                      },
                      {
                        "id":"ccc",
                        "tags":[
                          {
                            "name":"severity",
                            "value":"critical"
                          },
                          {
                            "name":"alertname",
                            "value":"DiskUsage"
                          }
                        ],
                        "status":"Ok"
                      }
                    ]
                  }
                }
                """ ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSelectAttrForArrayInArrayInArray() {

        String JSON_ARRAY_IN_ARRAY_IN_ARRAY =
                """
                {
                  "rules":[
                    {
                      "Rule":{
                        "name":"r1",
                        "condition":{
                          "AllCondition":[
                            {
                              "SelectAttrExpression":{
                                "lhs":{
                                  "Event":"incident.alerts.tags.messages"
                                },
                                "rhs":{
                                  "key":{
                                    "String":"value"
                                  },
                                  "operator":{
                                    "String":"=="
                                  },
                                  "value":{
                                    "String":"DiskUsage"
                                  }
                                }
                              }
                            }
                          ]
                        },
                        "actions":[
                          {
                            "Action":{
                              "action":"debug",
                              "action_args":{
                                "msg":"Found a match with alerts"
                              }
                            }
                          }
                        ],
                        "enabled":true
                      }
                    }
                  ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_ARRAY_IN_ARRAY_IN_ARRAY);

        List<Match> matchedRules = rulesExecutor.processFacts( """
                {
                  "incident":{
                    "id":"aaa",
                    "active":false,
                    "alerts":[
                      {
                        "id":"bbb",
                        "tags":[
                          {
                            "messages":[
                              {
                                "name":"alertname",
                                "value":"MariadbDown"
                              },
                              {
                                "name":"severity",
                                "value":"critical"
                              }
                            ]
                          },
                          {
                            "messages":[
                              {
                                "name":"severity",
                                "value":"low"
                              },
                              {
                                "name":"notification",
                                "value":"access"
                              }
                            ]
                          }
                        ],
                        "status":"Ok"
                      },
                      {
                        "id":"ccc",
                        "tags":[
                          {
                            "messages":[
                              {
                                "name":"severity",
                                "value":"critical"
                              },
                              {
                                "name":"alertname",
                                "value":"DiskUsage"
                              }
                            ]
                          },
                          {
                            "messages":[
                              {
                                "name":"severity",
                                "value":"low"
                              },
                              {
                                "name":"notification",
                                "value":"access"
                              }
                            ]
                          }
                        ],
                        "status":"Ok"
                      }
                    ]
                  }
                }
                """ ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Ignore("At the moment, non-index-array ('alerts') does not support two-dimensional arrays.")
    @Test
    public void testSelectAttrForTwoDimensionArray() {

        String JSON_TWO_DIMENSION_ARRAY =
                """
                {
                  "rules":[
                    {
                      "Rule":{
                        "name":"r1",
                        "condition":{
                          "AllCondition":[
                            {
                              "SelectAttrExpression":{
                                "lhs":{
                                  "Event":"incident.alerts.tags"
                                },
                                "rhs":{
                                  "key":{
                                    "String":"value"
                                  },
                                  "operator":{
                                    "String":"=="
                                  },
                                  "value":{
                                    "String":"DiskUsage"
                                  }
                                }
                              }
                            }
                          ]
                        },
                        "actions":[
                          {
                            "Action":{
                              "action":"debug",
                              "action_args":{
                                "msg":"Found a match with alerts"
                              }
                            }
                          }
                        ],
                        "enabled":true
                      }
                    }
                  ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_TWO_DIMENSION_ARRAY);

        List<Match> matchedRules = rulesExecutor.processFacts( """
                {
                  "incident":{
                    "id":"aaa",
                    "active":false,
                    "alerts":[
                      [
                        {
                          "name":"alertname",
                          "value":"MariadbDown"
                        },
                        {
                          "name":"severity",
                          "value":"critical"
                        }
                      ],
                      [
                        {
                          "name":"severity",
                          "value":"critical"
                        },
                        {
                          "name":"alertname",
                          "value":"DiskUsage"
                        }
                      ]
                    ]
                  }
                }
                """ ).join();

        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testListContainsForArrayInArray() {

        String JSON_ARRAY_IN_ARRAY =
                """
                {
                  "rules":[
                    {
                      "Rule":{
                        "name":"r1",
                        "condition":{
                          "AllCondition":[
                            {
                              "ListContainsItemExpression":{
                                "lhs":{
                                  "Event":"incident.alerts.tags"
                                },
                                "rhs":{
                                    "String":"DiskUsage"
                                }
                              }
                            }
                          ]
                        },
                        "actions":[
                          {
                            "Action":{
                              "action":"debug",
                              "action_args":{
                                "msg":"Found a match with alerts"
                              }
                            }
                          }
                        ],
                        "enabled":true
                      }
                    }
                  ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_ARRAY_IN_ARRAY);

        List<Match> matchedRules = rulesExecutor.processFacts( """
                {
                  "incident":{
                    "id":"aaa",
                    "active":false,
                    "alerts":[
                      {
                        "id":"bbb",
                        "tags":[
                          "MariadbDown",
                          "Hello"
                        ],
                        "status":"Ok"
                      },
                      {
                        "id":"ccc",
                        "tags":[
                          "Good Bye",
                          "DiskUsage"
                        ],
                        "status":"Ok"
                      }
                    ]
                  }
                }
                """ ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSelectForArrayInArray() {

        String JSON_ARRAY_IN_ARRAY =
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
                                                            "Event": "persons.levels"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_ARRAY_IN_ARRAY);

        List<Match> matchedRules = rulesExecutor.processFacts("""
                                                                      {
                                                                        "persons":[
                                                                          {
                                                                            "name":"Fred",
                                                                            "age":54,
                                                                            "levels":[
                                                                              10,
                                                                              20,
                                                                              30
                                                                            ]
                                                                          },
                                                                          {
                                                                            "name":"John",
                                                                            "age":36,
                                                                            "levels":[
                                                                              10,
                                                                              16
                                                                            ]
                                                                          }
                                                                        ]
                                                                      }
                                                                      """).join();
        assertEquals(1, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    public void testSimpleOperatorWithArrayCollectAsLeafNode_shouldFail() {
        String JSON_ARRAY_IN_ARRAY =
                """
                {
                    "rules": [
                            {
                                "Rule": {
                                    "condition": {
                                        "AllCondition": [
                                            {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Fact": "os.array[1].versions"
                                                    },
                                                    "rhs": {
                                                        "String": "Vista"
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    "enabled": true,
                                    "name": null
                                }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_ARRAY_IN_ARRAY);

        List<Match> matchedRules = rulesExecutor.processFacts( """
                {
                   "host":"B",
                   "os":{
                      "array":[
                         {
                            "name":"abc",
                            "versions":"Unknown"
                         },
                         {
                            "name":"windows",
                            "versions":["XP", "Millenium", "Vista"]
                         }
                      ]
                   }
                }
                """ ).join();

        // "os.array[1].versions" returns a list, so it hits a type mismatch error "list and str"
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testSimpleOperatorWithArrayCollectAsIntermediateNode_shouldFail() {
        String JSON_ARRAY_IN_ARRAY =
                """
                {
                    "rules": [
                            {
                                "Rule": {
                                    "condition": {
                                        "AllCondition": [
                                            {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Fact": "os.array.versions[2]"
                                                    },
                                                    "rhs": {
                                                        "String": "Vista"
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    "enabled": true,
                                    "name": null
                                }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_ARRAY_IN_ARRAY);

        List<Match> matchedRules = rulesExecutor.processFacts( """
                {
                   "host":"B",
                   "os":{
                      "array":[
                         {
                            "name":"abc",
                            "versions":"Unknown"
                         },
                         {
                            "name":"windows",
                            "versions":["XP", "Millenium", "Vista"]
                         }
                      ]
                   }
                }
                """ ).join();

        // "os.array.versions[2]" returns a list, because 'array' means all elements in 'array'.
        // So it hits a type mismatch error "list and str"
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }
}
