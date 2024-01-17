package org.drools.ansible.rulebook.integration.api;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.protoextractor.prototype.ExtractorPrototypeExpressionUtils;
import org.drools.model.Index;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;

import static org.drools.model.prototype.PrototypeExpression.fixedValue;
import static org.junit.Assert.assertEquals;

public class AdditionTest {

    @Test
    public void testExecuteRules() {
        String json =
                """
                {
                    "rules": [
                            {
                                "Rule": {
                                    "action": {
                                        "Action": {
                                            "action": "debug",
                                            "action_args": {}
                                        }
                                    },
                                    "condition": {
                                        "AllCondition": [
                                            {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "nested.i"
                                                    },
                                                    "rhs": {
                                                        "AdditionExpression": {
                                                            "lhs": {
                                                                "Event": "nested.j"
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
                                    "enabled": true,
                                    "name": null
                                }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"nested\": { \"i\": 1, \"j\":2 } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"nested\": { \"i\": 2, \"j\":1 } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testExecuteRulesSet() {
        RulesSet rulesSet = new RulesSet();
        rulesSet.addRule().withCondition().all()
                .addSingleCondition(ExtractorPrototypeExpressionUtils.prototypeFieldExtractor("nested.i"), Index.ConstraintType.EQUAL, ExtractorPrototypeExpressionUtils.prototypeFieldExtractor("nested.j").add(fixedValue(1)));

        RulesExecutor rulesExecutor = RulesExecutorFactory.createRulesExecutor(rulesSet);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"nested\": { \"i\": 1, \"j\":2 } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"nested\": { \"i\": 2, \"j\":1 } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testAdditionOnDifferentEvents() {
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
                                            "AssignmentExpression": {
                                                "lhs": {
                                                    "Events": "abc"
                                                },
                                                "rhs": {
                                                    "EqualsExpression": {
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
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "i"
                                                },
                                                "rhs": {
                                                    "Integer": 3
                                                }
                                            }
                                        },
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "i"
                                                },
                                                "rhs": {
                                                    "AdditionExpression": {
                                                        "lhs": {
                                                            "Events": "abc.i"
                                                        },
                                                        "rhs": {
                                                            "Events": "m_1.i"
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

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\": 1 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"i\": 2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"i\": 3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"i\": 4 }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"i\": 5 }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }
}
