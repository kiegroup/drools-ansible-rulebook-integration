package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ImplicitBindingTest {

    @Test
    void testAllCondition() {
        String JSON1 =
                """
                  {
                      "rules": [
                        {
                          "Rule": {
                            "condition": {
                              "AllCondition": [
                                {
                                  "AndExpression": {
                                    "lhs": {
                                      "AndExpression": {
                                        "lhs": {
                                          "GreaterThanExpression": {
                                            "lhs": {
                                              "Event": "i"
                                            },
                                            "rhs": {
                                              "Integer": 0
                                            }
                                          }
                                        },
                                        "rhs": {
                                          "GreaterThanExpression": {
                                            "lhs": {
                                              "Event": "i"
                                            },
                                            "rhs": {
                                              "Integer": 1
                                            }
                                          }
                                        }
                                      }
                                    },
                                    "rhs": {
                                      "GreaterThanExpression": {
                                        "lhs": {
                                          "Event": "i"
                                        },
                                        "rhs": {
                                          "Integer": 3
                                        }
                                      }
                                    }
                                  }
                                }
                              ]
                            },
                            "enabled": true,
                            "name": "r1"
                          }
                        }
                      ]
                    }
                  }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":67 }" ).join();
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );
        assertEquals( "m", matchedRules.get(0).getDeclarationIds().get(0) );
        rulesExecutor.dispose();
    }

    @Test
    void testAnyCondition() {
        String JSON1 =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "condition":{
                               "AnyCondition":[
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"i"
                                        },
                                        "rhs":{
                                           "Integer":0
                                        }
                                     }
                                  },
                                  {
                                     "GreaterThanExpression":{
                                        "lhs":{
                                           "Event":"i"
                                        },
                                        "rhs":{
                                           "Integer":1
                                        }
                                     }
                                  }
                               ]
                            },
                            "enabled":true,
                            "name":"r1"
                         }
                      }
                   ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts("{ \"i\":1 }").join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":67 }" ).join();
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );
        assertEquals( "m_1", matchedRules.get(0).getDeclarationIds().get(0) );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":0 }" ).join();
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );
        assertEquals( "m_0", matchedRules.get(0).getDeclarationIds().get(0) );
        rulesExecutor.dispose();
    }

    @Test
    void testAnyConditionWithPartialBinding() {
        String JSON1 =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "condition":{
                               "AnyCondition":[
                                  {
                                     "AssignmentExpression":{
                                        "lhs":{
                                           "Facts":"first"
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
                                     "GreaterThanExpression":{
                                        "lhs":{
                                           "Event":"i"
                                        },
                                        "rhs":{
                                           "Integer":1
                                        }
                                     }
                                  }
                               ]
                            },
                            "enabled":true,
                            "name":"r1"
                         }
                      }
                   ]
                }
                """;

        System.out.println(JSON1);

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts("{ \"i\":1 }").join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":67 }" ).join();
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );
        assertEquals( "m_1", matchedRules.get(0).getDeclarationIds().get(0) );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":0 }" ).join();
        assertEquals( "r1", matchedRules.get(0).getRule().getName() );
        assertEquals( "first", matchedRules.get(0).getDeclarationIds().get(0) );
        rulesExecutor.dispose();
    }

    @Test
    void testWithOr() {
        String JSON1 =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "name":"r1",
                            "condition":{
                               "AllCondition":[
                                  {
                                     "OrExpression":{
                                        "lhs":{
                                           "EqualsExpression":{
                                              "lhs":{
                                                 "Event":"nested.i"
                                              },
                                              "rhs":{
                                                 "Integer":1
                                              }
                                           }
                                        },
                                        "rhs":{
                                           "EqualsExpression":{
                                              "lhs":{
                                                 "Event":"nested.j"
                                              },
                                              "rhs":{
                                                 "Integer":1
                                              }
                                           }
                                        }
                                     }
                                  }
                               ]
                            },
                            "action":{
                               "Action":{
                                  "action":"debug",
                                  "action_args":{
                                    \s
                                  }
                               }
                            },
                            "enabled":true
                         }
                      }
                   ]
                }
                """;

        System.out.println(JSON1);

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts("{ \"nested\" : { \"i\" : 1 } }").join();
        assertEquals( 1, matchedRules.size() );

        assertEquals( "r1", matchedRules.get(0).getRule().getName() );
        assertEquals( "m", matchedRules.get(0).getDeclarationIds().get(0) );
        rulesExecutor.dispose();
    }
}
