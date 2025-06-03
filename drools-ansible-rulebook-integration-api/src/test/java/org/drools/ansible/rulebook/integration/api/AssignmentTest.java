package org.drools.ansible.rulebook.integration.api;

import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssignmentTest {

    @Test
    void testAssignmentReferenceEqualsExpressionRHS() {
        String JSON =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "action":{
                                       "Action":{
                                          "action":"debug",
                                          "action_args":{
                                            \s
                                          }
                                       }
                                    },
                                    "condition":{
                                       "AllCondition":[
                                          {
                                             "AssignmentExpression":{
                                                "lhs":{
                                                   "Events":"varname"
                                                },
                                                "rhs":{
                                                   "EqualsExpression":{
                                                      "lhs":{
                                                         "Event":"i"
                                                      },
                                                      "rhs":{
                                                         "Integer":0
                                                      }
                                                   }
                                                }
                                             }
                                          },
                                          {
                                              "EqualsExpression":{
                                                 "lhs":{
                                                    "Event":"j"
                                                 },
                                                 "rhs":{
                                                    "Events":"varname.i"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON);

        rulesExecutor.processEvents("{ \"i\": 0 }").join();
        List<Match> matchedRules = rulesExecutor.processFacts("{ \"j\": 0 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals("varname", matchedRules.get(0).getDeclarationIds().get(0));

        rulesExecutor.dispose();
    }

    @Test
    void testAssignmentReferenceEqualsExpressionLHS() {
        String JSON =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "action":{
                                       "Action":{
                                          "action":"debug",
                                          "action_args":{
                                            \s
                                          }
                                       }
                                    },
                                    "condition":{
                                       "AllCondition":[
                                          {
                                             "AssignmentExpression":{
                                                "lhs":{
                                                   "Events":"varname"
                                                },
                                                "rhs":{
                                                   "EqualsExpression":{
                                                      "lhs":{
                                                         "Event":"i"
                                                      },
                                                      "rhs":{
                                                         "Integer":0
                                                      }
                                                   }
                                                }
                                             }
                                          },
                                          {
                                              "EqualsExpression":{
                                                 "lhs":{
                                                    "Events":"varname.i"
                                                 },
                                                 "rhs":{
                                                    "Event":"j"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON);

        rulesExecutor.processEvents("{ \"i\": 0 }").join();
        List<Match> matchedRules = rulesExecutor.processFacts("{ \"j\": 0 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals("varname", matchedRules.get(0).getDeclarationIds().get(0));

        rulesExecutor.dispose();
    }

    @Test
    void testAssignmentReferenceItemInListExpressionLHS() {
        String JSON =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "action":{
                                       "Action":{
                                          "action":"debug",
                                          "action_args":{
                                            \s
                                          }
                                       }
                                    },
                                    "condition":{
                                       "AllCondition":[
                                        {
                                            "AssignmentExpression": {
                                                "lhs": {
                                                    "Events": "varname"
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "xyz"
                                                        },
                                                        "rhs": {
                                                            "Integer": 300
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "ItemInListExpression": {
                                                "lhs": {
                                                    "Events": "varname.pr"
                                                },
                                                "rhs": {
                                                    "Event": "prs_list"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON);

        rulesExecutor.processEvents("{ \"xyz\": 300, \"pr\": 456 }").join();
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"prs_list\": [ 456, 457 ] }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals("varname", matchedRules.get(0).getDeclarationIds().get(0));

        rulesExecutor.dispose();
    }

    @Test
    void testAssignmentReferenceListContainsItemExpressionRHS() {
        String JSON =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "action":{
                                       "Action":{
                                          "action":"debug",
                                          "action_args":{
                                            \s
                                          }
                                       }
                                    },
                                    "condition":{
                                       "AllCondition":[
                                        {
                                            "AssignmentExpression": {
                                                "lhs": {
                                                    "Events": "varname"
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "xyz"
                                                        },
                                                        "rhs": {
                                                            "Integer": 300
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "ListContainsItemExpression": {
                                                "lhs": {
                                                    "Event": "prs_list"
                                                },
                                                "rhs": {
                                                    "Events": "varname.pr"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON);

        rulesExecutor.processEvents("{ \"xyz\": 300, \"pr\": 456 }").join();
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"prs_list\": [ 456, 457 ] }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals("varname", matchedRules.get(0).getDeclarationIds().get(0));

        rulesExecutor.dispose();
    }

    @Test
    void testAssignmentReferenceListContainsItemExpressionLHS() {
        String JSON =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "action":{
                                       "Action":{
                                          "action":"debug",
                                          "action_args":{
                                            \s
                                          }
                                       }
                                    },
                                    "condition":{
                                       "AllCondition":[
                                        {
                                            "AssignmentExpression": {
                                                "lhs": {
                                                    "Events": "varname"
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "xyz"
                                                        },
                                                        "rhs": {
                                                            "Integer": 300
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "ListContainsItemExpression": {
                                                "lhs": {
                                                    "Events": "varname.prs_list"
                                                },
                                                "rhs": {
                                                    "Event": "pr"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON);

        rulesExecutor.processEvents("{ \"xyz\": 300, \"prs_list\": [ 456, 457 ] }").join();
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"pr\": 456 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals("varname", matchedRules.get(0).getDeclarationIds().get(0));

        rulesExecutor.dispose();
    }

    @Test
    void testAssignmentReferenceListNotContainsItemExpressionLHS() {
        String JSON =
                """
                        {
                           "rules":[
                              {
                                 "Rule":{
                                    "action":{
                                       "Action":{
                                          "action":"debug",
                                          "action_args":{
                                            \s
                                          }
                                       }
                                    },
                                    "condition":{
                                       "AllCondition":[
                                        {
                                            "AssignmentExpression": {
                                                "lhs": {
                                                    "Events": "varname"
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "xyz"
                                                        },
                                                        "rhs": {
                                                            "Integer": 300
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "ListNotContainsItemExpression": {
                                                "lhs": {
                                                    "Events": "varname.prs_list"
                                                },
                                                "rhs": {
                                                    "Event": "pr"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON);

        rulesExecutor.processEvents("{ \"xyz\": 300, \"prs_list\": [ 456, 457 ] }").join();
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"pr\": 999 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals("varname", matchedRules.get(0).getDeclarationIds().get(0));

        rulesExecutor.dispose();
    }

    @Test
    void testAssignmentReferenceSelectExpressionLHS_shouldThrowException() {
        String JSON =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "r1",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "AssignmentExpression": {
                                                "lhs": {
                                                    "Events": "varname"
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "xyz"
                                                        },
                                                        "rhs": {
                                                            "Integer": 300
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "SelectExpression": {
                                                "lhs": {
                                                    "Events": "varname.levels"
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

        assertThatThrownBy(() -> {
            RulesExecutorFactory.createFromJson(JSON);
        })
                .as("SelectExpression doesn't support inversion")
                .isInstanceOf(UnsupportedOperationException.class);
    }
}