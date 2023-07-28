package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class BetaEqualsTest {

    @Test
    public void testExecuteRulesWithExplicitJoin() {
        String json =
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
                                           "Facts":"first"
                                        },
                                        "rhs":{
                                           "IsDefinedExpression":{
                                              "Fact":"custom.expected_index"
                                           }
                                        }
                                     }
                                  },
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"i"
                                        },
                                        "rhs":{
                                           "Facts":"first.custom.expected_index"
                                        }
                                     }
                                  }
                               ]
                            },
                            "enabled":true,
                            "name":null
                         }
                      }
                   ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts("{ \"custom\": { \"expected_index\": 2 } }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 3 }").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{ \"i\": 2 }").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r_0", matchedRules.get(0).getRule().getName());

        rulesExecutor.dispose();
    }
}