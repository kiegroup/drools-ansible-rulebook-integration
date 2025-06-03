package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ArrayAccessTest {
    public static final String JSON1 =
            """
            {
                "rules": [
                        {
                            "Rule": {
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Fact": "os.array[1]"
                                                },
                                                "rhs": {
                                                    "String": "windows"
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

    @Test
    void testArrayAccess() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{\"host\": \"A\", \"os\": {\"array\": [\"abc\"]}}" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{\"host\": \"B\", \"os\": {\"array\": [\"abc\", \"windows\"]}}" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    public static final String JSON2 =
            """
            {
                "rules": [
                        {
                            "Rule": {
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Fact": "os.array[1].versions[2]"
                                                },
                                                "rhs": {
                                                    "String": "Vista"
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

    @Test
    void testNestedArrayAccess() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON2);

        List<Match> matchedRules = rulesExecutor.processFacts( """
                {
                   "host":"B",
                   "os":{
                      "array":[
                         {
                            "name":"abc",
                            "versions":"Unknown"
                         },
                         {
                            "name":"windows",
                            "versions":["XP", "Millenium"]
                         }
                      ]
                   }
                }
                """ ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( """
                {
                   "host":"B",
                   "os":{
                      "array":[
                         {
                            "name":"abc",
                            "versions":"Unknown"
                         },
                         {
                            "name":"windows",
                            "versions":["XP", "Millenium", "Vista"]
                         }
                      ]
                   }
                }
                """ ).join();

        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }
}
