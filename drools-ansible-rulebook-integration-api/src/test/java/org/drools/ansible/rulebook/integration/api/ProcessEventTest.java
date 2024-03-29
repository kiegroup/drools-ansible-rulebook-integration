package org.drools.ansible.rulebook.integration.api;

import org.junit.Test;
import org.kie.api.prototype.PrototypeFactInstance;
import org.kie.api.runtime.rule.Match;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProcessEventTest {

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

    @Test
    public void testExecuteRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\": 1 }" ).join();
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
                                                    "Fact": "os"
                                                },
                                                "rhs": {
                                                    "String": "linux"
                                                }
                                            }
                                        },
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
                        }
                    ]
            }
            """;

    @Test
    public void testEventShouldProduceMultipleMatchesForSameRule() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON2);

        rulesExecutor.processFacts( "{ \"host\": \"A\", \"os\": \"linux\" }" );
        rulesExecutor.processFacts( "{ \"host\": \"B\", \"os\": \"windows\" }" );
        rulesExecutor.processFacts( "{ \"host\": \"C\", \"os\": \"linux\" }" );

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\": 1 }" ).join();
        assertEquals( 2, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );
        assertEquals( "r_0", matchedRules.get(1).getRule().getName() );

        List<String> hosts = matchedRules.stream()
                .flatMap( m -> m.getObjects().stream() )
                .map( PrototypeFactInstance.class::cast)
                .filter( p -> p.has("host") )
                .map( p -> p.get("host") )
                .map( String.class::cast)
                .collect(Collectors.toList());

        assertEquals( 2, hosts.size() );
        assertTrue( hosts.containsAll(Arrays.asList("A", "C") ));

        rulesExecutor.dispose();
    }
}
