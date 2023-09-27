package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class AssignmentTest {

    @Test
    public void testAssignmentReferenceEqualsExpressionRHS() {
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

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": 0 }").join();
        matchedRules = rulesExecutor.processFacts("{ \"j\": 0 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals("varname", matchedRules.get(0).getDeclarationIds().get(0));

        rulesExecutor.dispose();
    }

    @Test
    public void testAssignmentReferenceEqualsExpressionLHS() {
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

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": 0 }").join();
        matchedRules = rulesExecutor.processFacts("{ \"j\": 0 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals("varname", matchedRules.get(0).getDeclarationIds().get(0));

        rulesExecutor.dispose();
    }

    @Test
    public void testAssignmentReferenceItemInListExpressionLHS() {
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

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"xyz\": 300, \"pr\": 456 }").join();
        matchedRules = rulesExecutor.processEvents("{ \"prs_list\": [ 456, 457 ] }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals("varname", matchedRules.get(0).getDeclarationIds().get(0));

        rulesExecutor.dispose();
    }

    @Test
    public void testAssignmentReferenceListContainsItemExpressionRHS() {
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

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"xyz\": 300, \"pr\": 456 }").join();
        matchedRules = rulesExecutor.processEvents("{ \"prs_list\": [ 456, 457 ] }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());
        assertEquals("varname", matchedRules.get(0).getDeclarationIds().get(0));

        rulesExecutor.dispose();
    }
}