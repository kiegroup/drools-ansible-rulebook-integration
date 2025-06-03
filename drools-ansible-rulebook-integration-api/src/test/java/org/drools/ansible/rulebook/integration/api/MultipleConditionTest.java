package org.drools.ansible.rulebook.integration.api;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultipleConditionTest {

    public static final String JSON1 =
            """
            {
               "rules":[
                  {
                     "Rule":{
                        "condition":{
                           "AllCondition":[
                              {
                                 "AssignmentExpression":{
                                    "lhs":{
                                       "Events":"first"
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
                                       "Events":"second"
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
                              },
                              {
                                 "AssignmentExpression":{
                                    "lhs":{
                                       "Events":"third"
                                    },
                                    "rhs":{
                                       "EqualsExpression":{
                                          "lhs":{
                                             "Event":"i"
                                          },
                                          "rhs":{
                                             "AdditionExpression":{
                                                "lhs":{
                                                   "Events":"first.i"
                                                },
                                                "rhs":{
                                                   "Integer":2
                                                }
                                             }
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

    @Test
    void testReadJson() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"events\": [ { \"i\":0 }, { \"i\":1 }, { \"i\":2 } ] }" ).join();

        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );

        RuleMatch ruleMatch = RuleMatch.from(matchedRules.get(0));
        assertEquals(3, ruleMatch.getFactsSize());
        assertEquals(Map.of("i", 0), ruleMatch.getFact("first"));
        assertEquals(Map.of("i", 1), ruleMatch.getFact("second"));
        assertEquals(Map.of("i", 2), ruleMatch.getFact("third"));

        rulesExecutor.dispose();
    }
}
