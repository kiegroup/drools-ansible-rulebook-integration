package org.drools.ansible.rulebook.integration.api;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.drools.base.common.NetworkNode;
import org.drools.core.reteoo.AlphaNode;
import org.drools.core.reteoo.ObjectSink;
import org.drools.kiesession.rulebase.InternalKnowledgeBase;
import org.drools.modelcompiler.constraints.LambdaConstraint;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kie.api.KieBase;
import org.kie.api.runtime.rule.Match;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class TypeMismatchTest {

    // For logging assertion
    static PrintStream originalOut = System.out;
    static StringPrintStream stringPrintStream = new StringPrintStream(System.out);

    @BeforeClass
    public static void beforeClass() {
        System.setOut(stringPrintStream);
    }

    @AfterClass
    public static void afterClass() {
        System.setOut(originalOut);
    }

    @Before
    public void before() {
        stringPrintStream.getStringList().clear();
    }

    public static final String JSON1 =
            """
                    {
                        "name": "ruleSet1",
                        "rules": [
                             {
                                 "Rule": {
                                     "name": "r1",
                                     "condition": {
                                         "AllCondition": [
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "meta.headers"
                                                     },
                                                     "rhs": {
                                                         "String": "Hello Testing World"
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
                             },
                             {
                                 "Rule": {
                                     "name": "r2",
                                     "condition": {
                                         "AllCondition": [
                                             {
                                                 "NotEqualsExpression": {
                                                     "lhs": {
                                                         "Event": "meta.headers"
                                                     },
                                                     "rhs": {
                                                         "String": "Hello Testing World"
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

    @Test
    public void mapAndString() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        // incoming event.mera.headers is a map, not a string
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"meta\": {\"headers\": {\"Content-Length\": \"36\"}} } }").join();
        // When comparing mismatched types, the rule should not match (even r2). Logs error for 2 rules.
        assertNumberOfErrorLogs(2);
        assertThat(stringPrintStream.getStringList())
                .anyMatch(s -> s.contains("Cannot compare values of different types: dict and str." +
                                                  " RuleSet: ruleSet1." +
                                                  " RuleName: r1." +
                                                  " Condition: {lhs={Event=meta.headers}, rhs={String=Hello Testing World}}"))
                .anyMatch(s -> s.contains("Cannot compare values of different types: dict and str." +
                                                  " RuleSet: ruleSet1." +
                                                  " RuleName: r2." +
                                                  " Condition: {lhs={Event=meta.headers}, rhs={String=Hello Testing World}}"));
        assertEquals(0, matchedRules.size());

        // One more time
        matchedRules = rulesExecutor.processEvents("{ \"meta\": {\"headers\": {\"Content-Length\": \"25\"}} } }").join();
        // not firing. Don't log errors again.
        assertNumberOfErrorLogs(2);
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    public static final String JSON2 =
            """
                    {
                        "name": "ruleSet1",
                        "rules": [
                             {
                                 "Rule": {
                                     "name": "r1",
                                     "condition": {
                                         "AllCondition": [
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "i"
                                                     },
                                                     "rhs": {
                                                         "Integer": 1
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

    @Test
    public void stringAndInteger() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON2);

        // incoming event.i is a string, not a integer
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": \"1\" }").join();
        assertNumberOfErrorLogs(1);
        assertThat(stringPrintStream.getStringList())
                .anyMatch(s -> s.contains("Cannot compare values of different types: str and int." +
                                                  " RuleSet: ruleSet1." +
                                                  " RuleName: r1." +
                                                  " Condition: {lhs={Event=i}, rhs={Integer=1}}"));
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    public void typeMismatchWithNodeSharing() {
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
                                                                    "Event": "i"
                                                                },
                                                                "rhs": {
                                                                    "Integer": 1
                                                                }
                                                            }
                                                        },
                                                        "rhs": {
                                                            "EqualsExpression": {
                                                                "lhs": {
                                                                    "Event": "j"
                                                                },
                                                                "rhs": {
                                                                    "Integer": 1
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
                                                    "action": "debug",
                                                    "action_args": {}
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
                                                    "AndExpression": {
                                                        "lhs": {
                                                            "EqualsExpression": {
                                                                "lhs": {
                                                                    "Event": "i"
                                                                },
                                                                "rhs": {
                                                                    "Integer": 1
                                                                }
                                                            }
                                                        },
                                                        "rhs": {
                                                            "EqualsExpression": {
                                                                "lhs": {
                                                                    "Event": "j"
                                                                },
                                                                "rhs": {
                                                                    "Integer": 2
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);
        KieBase kieBase = rulesExecutor.asKieSession().getKieBase();
        List<AlphaNode> alphaNodes = collectAlphaNodes(kieBase);
        // assert node sharing
        assertThat(alphaNodes.stream()
                           .map(node -> ((LambdaConstraint) node.getConstraint()).getEvaluator().getConstraint())
                           .filter(constraint -> constraint.getExprId().equals("expr:i:EQUAL:1"))
                           .count()).isEqualTo(1);

        // i is a string, not an integer
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": \"1\", \"j\": 1 }").join();
        assertNumberOfErrorLogs(1);
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    private List<AlphaNode> collectAlphaNodes(KieBase kieBase) {
        List<AlphaNode> alphaNodes = new ArrayList<>();
        ((InternalKnowledgeBase) kieBase).getRete().getObjectTypeNodes().forEach(otn -> {
            ObjectSink[] sinks = otn.getObjectSinkPropagator().getSinks();
            collectAlphaNodes(sinks, alphaNodes);
        });
        return alphaNodes;
    }

    private static void collectAlphaNodes(NetworkNode[] sinks, List<AlphaNode> alphaNodes) {
        if (sinks == null) {
            return;
        }
        for (NetworkNode sink : sinks) {
            if (sink instanceof AlphaNode alphaNode) {
                alphaNodes.add(alphaNode);
            }
            collectAlphaNodes(sink.getSinks(), alphaNodes);
        }
    }

    private static void assertNumberOfErrorLogs(int expected) {
        assertThat(stringPrintStream.getStringList().stream().filter(s -> s.contains("ERROR")).count()).isEqualTo(expected);
    }
}