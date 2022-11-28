package org.drools.ansible.rulebook.integration.api;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class OnceWithinTest {

    @Test
    public void testOnceWithinInCondition() {
        String json =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"condition\":{\n" +
                "               \"AllCondition\":[\n" +
                "                  {\n" +
                "                     \"AssignmentExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Events\":\"singleton\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"EqualsExpression\":{\n" +
                "                              \"lhs\":{\n" +
                "                                 \"Event\":\"sensu.process.type\"\n" +
                "                              },\n" +
                "                              \"rhs\":{\n" +
                "                                 \"String\":\"alert\"\n" +
                "                              }\n" +
                "                           }\n" +
                "                        }\n" +
                "                     }\n" +
                "                  }\n" +
                "               ],\n" +
                "               \"once_within\":\"10 seconds\",\n" +
                "               \"unique_attributes\":[\n" +
                "                  \"event.sensu.host\",\n" +
                "                  \"event.sensu.process.type\"\n" +
                "               ]\n" +
                "            },\n" +
                "            \"action\":{\n" +
                "               \"assert_fact\":{\n" +
                "                  \"ruleset\":\"Test rules4\",\n" +
                "                  \"fact\":{\n" +
                "                     \"j\":1\n" +
                "                  }\n" +
                "               }\n" +
                "            }\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        onceWithinTest(json);
    }

    @Test
    public void testOnceWithinInRule() {
        String json =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"condition\":{\n" +
                "               \"AllCondition\":[\n" +
                "                  {\n" +
                "                     \"AssignmentExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Events\":\"singleton\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"EqualsExpression\":{\n" +
                "                              \"lhs\":{\n" +
                "                                 \"Event\":\"sensu.process.type\"\n" +
                "                              },\n" +
                "                              \"rhs\":{\n" +
                "                                 \"String\":\"alert\"\n" +
                "                              }\n" +
                "                           }\n" +
                "                        }\n" +
                "                     }\n" +
                "                  }\n" +
                "               ]\n" +
                "            },\n" +
                "            \"action\":{\n" +
                "               \"assert_fact\":{\n" +
                "                  \"ruleset\":\"Test rules4\",\n" +
                "                  \"fact\":{\n" +
                "                     \"j\":1\n" +
                "                  }\n" +
                "               }\n" +
                "            },\n" +
                "            \"once_within\":\"10 seconds\",\n" +
                "            \"unique_attributes\":[\n" +
                "               \"event.sensu.host\",\n" +
                "               \"event.sensu.process.type\"\n" +
                "            ]\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        onceWithinTest(json);
    }

    private void onceWithinTest(String json) {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);
        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.advanceTime( 3, TimeUnit.SECONDS );

        matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h2\" } }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.advanceTime( 4, TimeUnit.SECONDS );

        matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.advanceTime( 5, TimeUnit.SECONDS );

        matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"h1\" } }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }
}
