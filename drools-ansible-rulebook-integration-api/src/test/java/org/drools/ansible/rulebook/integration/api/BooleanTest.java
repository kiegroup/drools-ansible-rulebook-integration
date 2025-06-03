package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BooleanTest {

    @Test
    void testProcessRuleWithBoolean() {
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
                                           "Boolean":true
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":true } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    void testProcessRuleWithImplicitBoolean() {
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
                                     "sensu":"data.i"
                                  }
                               ]
                            }
                         }
                      }
                   ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":3 } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":true } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    void testProcessRuleWithImplicitNegateBoolean() {
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
                                     "NegateExpression": { "sensu":"data.i" }
                                  }
                               ]
                            }
                         }
                      }
                   ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":true } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":false } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    void testProcessRuleWithLiteralBoolean() {
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
                                     "AndExpression":{
                                        "lhs":{
                                           "EqualsExpression":{
                                              "lhs":{
                                                 "Event":"i"
                                              },
                                              "rhs":{
                                                 "Integer":1
                                              }
                                           }
                                        },
                                        "rhs":{
                                           "Boolean":true
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":1 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }
}
