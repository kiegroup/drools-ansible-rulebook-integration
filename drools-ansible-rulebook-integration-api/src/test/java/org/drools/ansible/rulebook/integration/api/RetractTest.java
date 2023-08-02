package org.drools.ansible.rulebook.integration.api;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RetractTest {

    @Test
    public void testExecuteRules() {
        String JSON1 =
                """
                {
                           "rules": [
                            {
                                "Rule": {
                                    "action": {
                                        "Action": {
                                            "action": "assert_fact",
                                            "action_args": {
                                                "fact": {
                                                    "msg": "hello world"
                                                }
                                            }
                                        }
                                    },
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
                                    "enabled": true,
                                    "name": null
                                }
                            },
                            {
                                "Rule": {
                                    "action": {
                                        "Action": {
                                            "action": "retract_fact",
                                            "action_args": {
                                                "fact": {
                                                    "msg": "hello world"
                                                }
                                            }
                                        }
                                    },
                                    "condition": {
                                        "AllCondition": [
                                            {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "msg"
                                                    },
                                                    "rhs": {
                                                        "String": "hello world"
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    "enabled": true,
                                    "name": null
                                }
                            },
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
                                                "IsNotDefinedExpression": {
                                                    "Event": "msg"
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"msg\" : \"hello world\" }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processRetractMatchingFacts( "{ \"msg\" : \"hello world\" }", false ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_2", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testRetract() {
        String JSON1 =
                """
                {
                           "rules": [
                            {
                                "Rule": {
                                    "condition": {
                                        "AllCondition": [
                                            {
                                                "GreaterThanExpression": {
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
                                    "enabled": true,
                                    "name": null
                                }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"facts\" : [ { \"i\" : 1 }, { \"i\" : 1, \"j\" : 1 }, { \"i\" : 2, \"j\" : 2 } ] }" ).join();
        assertEquals( 3, matchedRules.size() );
        assertEquals( 3, rulesExecutor.getAllFacts().size() );

        matchedRules = rulesExecutor.processRetractMatchingFacts( "{ \"i\" : 1 }", false ).join();
        assertEquals( 0, matchedRules.size() );
        assertEquals( 2, rulesExecutor.getAllFacts().size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testRetractMatchingFacts() {
        String JSON1 =
                """
                {
                           "rules": [
                            {
                                "Rule": {
                                    "condition": {
                                        "AllCondition": [
                                            {
                                                "GreaterThanExpression": {
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
                                    "enabled": true,
                                    "name": null
                                }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"facts\" : [ { \"i\" : 1 }, { \"i\" : 1, \"j\" : 1 }, { \"i\" : 2, \"j\" : 2 } ] }" ).join();
        assertEquals( 3, matchedRules.size() );
        assertEquals( 3, rulesExecutor.getAllFacts().size() );

        matchedRules = rulesExecutor.processRetractMatchingFacts( "{ \"i\" : 1 }", true ).join();
        assertEquals( 0, matchedRules.size() );
        assertEquals( 1, rulesExecutor.getAllFacts().size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testRetractMatchingFactsIgnoringKeys() {
        String JSON1 =
                """
                {
                           "rules": [
                            {
                                "Rule": {
                                    "condition": {
                                        "AllCondition": [
                                            {
                                                "GreaterThanExpression": {
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
                                    "enabled": true,
                                    "name": null
                                }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( """
                { "facts" : [ 
                { "i" : 1, "x" : 1, "y" : 1 }, 
                { "i" : 1, "x" : { "i" : 1, "j" : 1 } }, 
                { "i" : 1, "j" : 1 }, 
                { "i" : 2, "x" : 1 } ] }
                """ ).join();

        assertEquals( 4, matchedRules.size() );
        assertEquals( 4, rulesExecutor.getAllFacts().size() );

        matchedRules = rulesExecutor.processRetractMatchingFacts( "{ \"i\" : 1 }", false, "x", "y" ).join();
        assertEquals( 0, matchedRules.size() );
        assertEquals( 2, rulesExecutor.getAllFacts().size() );

        rulesExecutor.dispose();
    }
}
