package org.drools.ansible.rulebook.integration.api;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.protoextractor.prototype.ExtractorPrototypeExpressionUtils;
import org.drools.model.Index;
import org.drools.model.prototype.PrototypeFact;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;
import java.util.Map;

import static org.drools.ansible.rulebook.integration.api.ObjectMapperFactory.createMapper;
import static org.drools.model.prototype.PrototypeExpression.fixedValue;
import static org.junit.Assert.assertEquals;

public class LogicalOperatorsTest {

    private static final String JSON1 =
            """
            {
               "rules":[
                  {
                     "Rule":{
                        "name":"R1",
                        "condition":{
                           "EqualsExpression":{
                              "lhs":{
                                 "sensu":"data.i"
                              },
                              "rhs":{
                                 "Integer":1
                              }
                           }
                        }
                     }
                  },
                  {
                     "Rule":{
                        "name":"R2",
                        "condition":{
                           "AllCondition":[
                              {
                                 "EqualsExpression":{
                                    "lhs":{
                                       "sensu":"data.i"
                                    },
                                    "rhs":{
                                       "Integer":3
                                    }
                                 }
                              },
                              {
                                 "EqualsExpression":{
                                    "lhs":"j",
                                    "rhs":{
                                       "Integer":2
                                    }
                                 }
                              }
                           ]
                        }
                     }
                  },
                  {
                     "Rule":{
                        "name":"R3",
                        "condition":{
                           "AnyCondition":[
                              {
                                 "AllCondition":[
                                    {
                                       "EqualsExpression":{
                                          "lhs":{
                                             "sensu":"data.i"
                                          },
                                          "rhs":{
                                             "Integer":3
                                          }
                                       }
                                    },
                                    {
                                       "EqualsExpression":{
                                          "lhs":"j",
                                          "rhs":{
                                             "Integer":2
                                          }
                                       }
                                    }
                                 ]
                              },
                              {
                                 "AllCondition":[
                                    {
                                       "EqualsExpression":{
                                          "lhs":{
                                             "sensu":"data.i"
                                          },
                                          "rhs":{
                                             "Integer":4
                                          }
                                       }
                                    },
                                    {
                                       "EqualsExpression":{
                                          "lhs":"j",
                                          "rhs":{
                                             "Integer":3
                                          }
                                       }
                                    }
                                 ]
                              }
                           ]
                        }
                     }
                  }
               ]
            }
            """;

    @Test
    public void testReadJson() throws JsonProcessingException {
        System.out.println(JSON1);
        ObjectMapper mapper = createMapper(new JsonFactory());
        RulesSet rulesSet = mapper.readValue(JSON1, RulesSet.class);
        System.out.println(rulesSet);
        String json = mapper.writerFor(RulesSet.class).writeValueAsString(rulesSet);
        System.out.println(json);
    }

    @Test
    public void testProcessRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"facts\": [ { \"sensu\": { \"data\": { \"i\":3 } } }, { \"j\":3 } ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":4 } } }" ).join();
        assertEquals( 1, matchedRules.size() );

        RuleMatch ruleMatch = RuleMatch.from( matchedRules.get(0) );
        assertEquals( "R3", ruleMatch.getRuleName() );
        assertEquals( 3, ((Map) ruleMatch.getFact("m_3")).get("j") );

        assertEquals( 4, ((Map) ((Map) ((Map) ruleMatch.getFact("m_2")).get("sensu")).get("data")).get("i") );

        rulesExecutor.dispose();
    }

    @Test
    public void testMultipleConditionOnSameFact() {
        String JSON2 =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "condition":{
                               "AllCondition":[
                                  {
                                     "AndExpression":{
                                        "lhs":{
                                           "AndExpression":{
                                              "lhs":{
                                                 "GreaterThanExpression":{
                                                    "lhs":{
                                                       "Event":"i"
                                                    },
                                                    "rhs":{
                                                       "Integer":0
                                                    }
                                                 }
                                              },
                                              "rhs":{
                                                 "GreaterThanExpression":{
                                                    "lhs":{
                                                       "Event":"i"
                                                    },
                                                    "rhs":{
                                                       "Integer":1
                                                    }
                                                 }
                                              }
                                           }
                                        },
                                        "rhs":{
                                           "GreaterThanExpression":{
                                              "lhs":{
                                                 "Event":"i"
                                              },
                                              "rhs":{
                                                 "Integer":3
                                              }
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON2);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":1 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":4 }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testOr() {
        String json =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "condition":{
                               "AllCondition":[
                                  {
                                     "OrExpression":{
                                        "lhs":{
                                           "LessThanExpression":{
                                              "lhs":{
                                                 "Event":"i"
                                              },
                                              "rhs":{
                                                 "Integer":1
                                              }
                                           }
                                        },
                                        "rhs":{
                                           "GreaterThanExpression":{
                                              "lhs":{
                                                 "Event":"i"
                                              },
                                              "rhs":{
                                                 "Integer":3
                                              }
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":0 }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":4 }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testNegate() {
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
                                                    "NegateExpression": {
                                                        "AndExpression": {
                                                            "lhs": {
                                                                "GreaterThanExpression": {
                                                                    "lhs": {
                                                                        "Event": "i"
                                                                    },
                                                                    "rhs": {
                                                                        "Integer": 4
                                                                    }
                                                                }
                                                            },
                                                            "rhs": {
                                                                "LessThanExpression": {
                                                                    "lhs": {
                                                                        "Event": "i"
                                                                    },
                                                                    "rhs": {
                                                                        "Integer": 10
                                                                    }
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
                                        "enabled": true
                                    }
                                }
                            ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":7 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":2 }" ).join();
        assertEquals( 1, matchedRules.size() );

        Match match = matchedRules.get(0);
        assertEquals( "r1", match.getRule().getName() );
        assertEquals( 2, ((PrototypeFact)match.getDeclarationValue("m")).get("i") );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":14 }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testAny() {
        RulesSet rulesSet = new RulesSet();
        rulesSet.addRule().withCondition().any()
                .addSingleCondition(ExtractorPrototypeExpressionUtils.prototypeFieldExtractor("i"), Index.ConstraintType.EQUAL, fixedValue(0)).withPatternBinding("event")
                .addSingleCondition(ExtractorPrototypeExpressionUtils.prototypeFieldExtractor("i"), Index.ConstraintType.EQUAL, fixedValue(1)).withPatternBinding("event");

        RulesExecutor rulesExecutor = RulesExecutorFactory.createRulesExecutor(rulesSet);
        checkAnyExecution(rulesExecutor);
    }

    @Test
    public void testAnyWithJson() {
        String JSON4 =
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
                               "AnyCondition":[
                                  {
                                     "AssignmentExpression":{
                                        "lhs":{
                                           "Events":"event"
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
                                     "AssignmentExpression":{
                                        "lhs":{
                                           "Events":"event"
                                        },
                                        "rhs":{
                                           "EqualsExpression":{
                                              "lhs":{
                                                 "Event":"i"
                                              },
                                              "rhs":{
                                                 "Integer":1
                                              }
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON4);
        checkAnyExecution(rulesExecutor);

        rulesExecutor.dispose();
    }

    private void checkAnyExecution(RulesExecutor rulesExecutor) {
        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\": 2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\": 1 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );
        assertEquals( "event", matchedRules.get(0).getDeclarationIds().get(0) );

        matchedRules = rulesExecutor.processFacts( "{ \"i\": 0 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );
        assertEquals( "event", matchedRules.get(0).getDeclarationIds().get(0) );

        rulesExecutor.dispose();
    }

    @Test
    public void testOrWithNestedAnd() {
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
                                                    "OrExpression": {
                                                        "lhs": {
                                                            "AndExpression": {
                                                                "lhs": {
                                                                    "GreaterThanExpression": {
                                                                        "lhs": {
                                                                            "Event": "i"
                                                                        },
                                                                        "rhs": {
                                                                            "Integer": 2
                                                                        }
                                                                    }
                                                                },
                                                                "rhs": {
                                                                    "LessThanExpression": {
                                                                        "lhs": {
                                                                            "Event": "i"
                                                                        },
                                                                        "rhs": {
                                                                            "Integer": 4
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        },
                                                        "rhs": {
                                                            "AndExpression": {
                                                                "lhs": {
                                                                    "LessThanExpression": {
                                                                        "lhs": {
                                                                            "Event": "i"
                                                                        },
                                                                        "rhs": {
                                                                            "Integer": 8
                                                                        }
                                                                    }
                                                                },
                                                                "rhs": {
                                                                    "GreaterThanExpression": {
                                                                        "lhs": {
                                                                            "Event": "i"
                                                                        },
                                                                        "rhs": {
                                                                            "Integer": 6
                                                                        }
                                                                    }
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
                                        "enabled": true
                                    }
                                }
                            ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":0 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":5 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":7 }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testPreventSelfJoin() {
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
                                     "EqualsExpression":{
                                        "lhs":{
                                           "sensu":"data.i"
                                        },
                                        "rhs":{
                                           "Integer":3
                                        }
                                     }
                                  },
                                  {
                                     "EqualsExpression":{
                                        "lhs":"j",
                                        "rhs":{
                                           "Integer":2
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

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"data\": { \"i\":3 } }, \"j\":2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"data\": { \"i\":3 } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"j\":2 }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testComparisonWithDifferentNumericTypes() {
        String json =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "echo",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "AndExpression": {
                                                "lhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "action"
                                                        },
                                                        "rhs": {
                                                            "String": "go"
                                                        }
                                                    }
                                                },
                                                "rhs": {
                                                    "GreaterThanExpression": {
                                                        "lhs": {
                                                            "Event": "i"
                                                        },
                                                        "rhs": {
                                                            "Float": 1.5
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

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\":3, \"action\":\"go\" }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testEqualityWithDifferentNumericTypes() {
        String json =
                """
                {
                    "rules": [
                        {
                            "Rule": {
                                "name": "echo",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "AndExpression": {
                                                "lhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "action"
                                                        },
                                                        "rhs": {
                                                            "String": "go"
                                                        }
                                                    }
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "i"
                                                        },
                                                        "rhs": {
                                                            "Float": 3.0
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

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\":3, \"action\":\"go\" }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"i\":3.00, \"action\":\"go\" }" ).join(); // 3.00 becomes BigDecimal("3.00") by RulesModelUtil.asFactMap()
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testOrWithAnd() {
        String json =
                """
        {
            "rules": [
                {
                    "Rule": {
                        "name": "Test and-or operator simple",
                        "condition": {
                            "AllCondition": [
                                {
                                    "AndExpression": {
                                        "lhs": {
                                            "OrExpression": {
                                                "lhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "myint"
                                                        },
                                                        "rhs": {
                                                            "Integer": 73
                                                        }
                                                    }
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
                                                        "lhs": {
                                                            "Event": "mystring"
                                                        },
                                                        "rhs": {
                                                            "String": "world"
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        "rhs": {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "mystring"
                                                },
                                                "rhs": {
                                                    "String": "hello"
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
                                    "action": "echo",
                                    "action_args": {
                                        "message": "Test and-or operator #1 passes"
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

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"id\": \"test_and_operator\", \"myint\": 73, \"mystring\": \"hello\" }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"id\": \"test_and_operator\", \"myint\": 73, \"mystring\": \"world\" }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }
}