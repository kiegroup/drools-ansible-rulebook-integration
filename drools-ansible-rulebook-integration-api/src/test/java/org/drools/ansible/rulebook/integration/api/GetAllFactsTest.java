package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GetAllFactsTest {

    @Test
    void test() {
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
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "i"
                                                },
                                                "rhs": {
                                                    "Integer": 0
                                                }
                                            }
                                        }
                                    ]
                                },
                                "actions": [
                                    {
                                        "Action": {
                                            "action": "set_fact",
                                            "action_args": {
                                                "fact": {
                                                    "status": "created"
                                                }
                                            }
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
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "i"
                                                },
                                                "rhs": {
                                                    "Integer": 4
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
                                "name": "r3",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Fact": "status"
                                                },
                                                "rhs": {
                                                    "String": "created"
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
                                                "message": "Fact matches"
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

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"i\": 0 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r1", matchedRules.get(0).getRule().getName());
        assertEquals(0, rulesExecutor.getAllFacts().size());

        matchedRules = rulesExecutor.processFacts("{ \"status\": \"created\" }").join();
        assertEquals(1, matchedRules.size());
        assertEquals(1, rulesExecutor.getAllFacts().size());

        rulesExecutor.dispose();
    }
}
