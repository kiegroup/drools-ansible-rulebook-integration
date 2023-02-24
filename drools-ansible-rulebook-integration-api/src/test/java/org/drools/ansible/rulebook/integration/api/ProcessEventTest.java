package org.drools.ansible.rulebook.integration.api;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.drools.model.PrototypeFact;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProcessEventTest {

    @Test
    public void testExecuteRules() {
        String JSON1 =
                "{\n" +
                "    \"rules\": [\n" +
                "            {\n" +
                "                \"Rule\": {\n" +
                "                    \"condition\": {\n" +
                "                        \"AllCondition\": [\n" +
                "                            {\n" +
                "                                \"EqualsExpression\": {\n" +
                "                                    \"lhs\": {\n" +
                "                                        \"Event\": \"i\"\n" +
                "                                    },\n" +
                "                                    \"rhs\": {\n" +
                "                                        \"Integer\": 1\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    },\n" +
                "                    \"enabled\": true,\n" +
                "                    \"name\": null\n" +
                "                }\n" +
                "            },\n" +
                "            {\n" +
                "                \"Rule\": {\n" +
                "                    \"condition\": {\n" +
                "                        \"AllCondition\": [\n" +
                "                            {\n" +
                "                                \"IsNotDefinedExpression\": {\n" +
                "                                    \"Event\": \"msg\"\n" +
                "                                }\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    },\n" +
                "                    \"enabled\": true,\n" +
                "                    \"name\": null\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\": 1 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testEventShouldProduceMultipleMatchesForSameRule() {

        String JSON2 =
                "{\n" +
                "    \"rules\": [\n" +
                "            {\n" +
                "                \"Rule\": {\n" +
                "                    \"condition\": {\n" +
                "                        \"AllCondition\": [\n" +
                "                            {\n" +
                "                                \"EqualsExpression\": {\n" +
                "                                    \"lhs\": {\n" +
                "                                        \"Fact\": \"os\"\n" +
                "                                    },\n" +
                "                                    \"rhs\": {\n" +
                "                                        \"String\": \"linux\"\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"EqualsExpression\": {\n" +
                "                                    \"lhs\": {\n" +
                "                                        \"Event\": \"i\"\n" +
                "                                    },\n" +
                "                                    \"rhs\": {\n" +
                "                                        \"Integer\": 1\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    },\n" +
                "                    \"enabled\": true,\n" +
                "                    \"name\": null\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "}";

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
                .map( PrototypeFact.class::cast)
                .filter( p -> p.has("host") )
                .map( p -> p.get("host") )
                .map( String.class::cast)
                .collect(Collectors.toList());

        assertEquals( 2, hosts.size() );
        assertTrue( hosts.containsAll(Arrays.asList("A", "C") ));

        rulesExecutor.dispose();
    }
}
