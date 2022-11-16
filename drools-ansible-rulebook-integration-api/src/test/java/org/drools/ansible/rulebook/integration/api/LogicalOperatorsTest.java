package org.drools.ansible.rulebook.integration.api;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.model.Index;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.api.ObjectMapperFactory.createMapper;
import static org.drools.model.PrototypeExpression.fixedValue;
import static org.drools.model.PrototypeExpression.prototypeField;
import static org.junit.Assert.assertEquals;

public class LogicalOperatorsTest {

    private static final String JSON1 =
            "{\n" +
            "   \"rules\":[\n" +
            "      {\n" +
            "         \"Rule\":{\n" +
            "            \"name\":\"R1\",\n" +
            "            \"condition\":{\n" +
            "               \"EqualsExpression\":{\n" +
            "                  \"lhs\":{\n" +
            "                     \"sensu\":\"data.i\"\n" +
            "                  },\n" +
            "                  \"rhs\":{\n" +
            "                     \"Integer\":1\n" +
            "                  }\n" +
            "               }\n" +
            "            }\n" +
            "         }\n" +
            "      },\n" +
            "      {\n" +
            "         \"Rule\":{\n" +
            "            \"name\":\"R2\",\n" +
            "            \"condition\":{\n" +
            "               \"AllCondition\":[\n" +
            "                  {\n" +
            "                     \"EqualsExpression\":{\n" +
            "                        \"lhs\":{\n" +
            "                           \"sensu\":\"data.i\"\n" +
            "                        },\n" +
            "                        \"rhs\":{\n" +
            "                           \"Integer\":3\n" +
            "                        }\n" +
            "                     }\n" +
            "                  },\n" +
            "                  {\n" +
            "                     \"EqualsExpression\":{\n" +
            "                        \"lhs\":\"j\",\n" +
            "                        \"rhs\":{\n" +
            "                           \"Integer\":2\n" +
            "                        }\n" +
            "                     }\n" +
            "                  }\n" +
            "               ]\n" +
            "            }\n" +
            "         }\n" +
            "      },\n" +
            "      {\n" +
            "         \"Rule\":{\n" +
            "            \"name\":\"R3\",\n" +
            "            \"condition\":{\n" +
            "               \"AnyCondition\":[\n" +
            "                  {\n" +
            "                     \"AllCondition\":[\n" +
            "                        {\n" +
            "                           \"EqualsExpression\":{\n" +
            "                              \"lhs\":{\n" +
            "                                 \"sensu\":\"data.i\"\n" +
            "                              },\n" +
            "                              \"rhs\":{\n" +
            "                                 \"Integer\":3\n" +
            "                              }\n" +
            "                           }\n" +
            "                        },\n" +
            "                        {\n" +
            "                           \"EqualsExpression\":{\n" +
            "                              \"lhs\":\"j\",\n" +
            "                              \"rhs\":{\n" +
            "                                 \"Integer\":2\n" +
            "                              }\n" +
            "                           }\n" +
            "                        }\n" +
            "                     ]\n" +
            "                  },\n" +
            "                  {\n" +
            "                     \"AllCondition\":[\n" +
            "                        {\n" +
            "                           \"EqualsExpression\":{\n" +
            "                              \"lhs\":{\n" +
            "                                 \"sensu\":\"data.i\"\n" +
            "                              },\n" +
            "                              \"rhs\":{\n" +
            "                                 \"Integer\":4\n" +
            "                              }\n" +
            "                           }\n" +
            "                        },\n" +
            "                        {\n" +
            "                           \"EqualsExpression\":{\n" +
            "                              \"lhs\":\"j\",\n" +
            "                              \"rhs\":{\n" +
            "                                 \"Integer\":3\n" +
            "                              }\n" +
            "                           }\n" +
            "                        }\n" +
            "                     ]\n" +
            "                  }\n" +
            "               ]\n" +
            "            }\n" +
            "         }\n" +
            "      }\n" +
            "   ]\n" +
            "}";

    @Test
    public void testReadJson() throws JsonProcessingException {
        System.out.println(JSON1);
        ObjectMapper mapper = createMapper(new JsonFactory());
        RulesSet rulesSet = mapper.readValue(JSON1, RulesSet.class);
        System.out.println(rulesSet);
        String json = mapper.writerFor(RulesSet.class).writeValueAsString(rulesSet);
        System.out.println(json);
    }

    @Test
    public void testProcessRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" );
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ facts: [ { \"sensu\": { \"data\": { \"i\":3 } } }, { \"j\":3 } ] }" );
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":4 } } }" );
        assertEquals( 1, matchedRules.size() );

        RuleMatch ruleMatch = RuleMatch.from( matchedRules.get(0) );
        assertEquals( "R3", ruleMatch.getRuleName() );
        assertEquals( 3, ruleMatch.getFacts().get("j") );

        assertEquals( 4, ((Map) ((Map) ruleMatch.getFacts().get("sensu")).get("data")).get("i") );

        rulesExecutor.dispose();
    }

    private static final String JSON2 =
            "{\n" +
            "   \"rules\":[\n" +
            "      {\n" +
            "         \"Rule\":{\n" +
            "            \"condition\":{\n" +
            "               \"AllCondition\":[\n" +
            "                  {\n" +
            "                     \"AndExpression\":{\n" +
            "                        \"lhs\":{\n" +
            "                           \"AndExpression\":{\n" +
            "                              \"lhs\":{\n" +
            "                                 \"GreaterThanExpression\":{\n" +
            "                                    \"lhs\":{\n" +
            "                                       \"Event\":\"i\"\n" +
            "                                    },\n" +
            "                                    \"rhs\":{\n" +
            "                                       \"Integer\":0\n" +
            "                                    }\n" +
            "                                 }\n" +
            "                              },\n" +
            "                              \"rhs\":{\n" +
            "                                 \"GreaterThanExpression\":{\n" +
            "                                    \"lhs\":{\n" +
            "                                       \"Event\":\"i\"\n" +
            "                                    },\n" +
            "                                    \"rhs\":{\n" +
            "                                       \"Integer\":1\n" +
            "                                    }\n" +
            "                                 }\n" +
            "                              }\n" +
            "                           }\n" +
            "                        },\n" +
            "                        \"rhs\":{\n" +
            "                           \"GreaterThanExpression\":{\n" +
            "                              \"lhs\":{\n" +
            "                                 \"Event\":\"i\"\n" +
            "                              },\n" +
            "                              \"rhs\":{\n" +
            "                                 \"Integer\":3\n" +
            "                              }\n" +
            "                           }\n" +
            "                        }\n" +
            "                     }\n" +
            "                  }\n" +
            "               ]\n" +
            "            }\n" +
            "         }\n" +
            "      }\n" +
            "   ]\n" +
            "}";

    @Test
    public void testMultipleConditionOnSameFact() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON2);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":1 }" );
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":2 }" );
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":3 }" );
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":4 }" );
        assertEquals( 1, matchedRules.size() );
    }

    private static final String JSON3 =
            "{\n" +
            "   \"rules\":[\n" +
            "      {\n" +
            "         \"Rule\":{\n" +
            "            \"condition\":{\n" +
            "               \"AllCondition\":[\n" +
            "                  {\n" +
            "                     \"OrExpression\":{\n" +
            "                        \"lhs\":{\n" +
            "                           \"LessThanExpression\":{\n" +
            "                              \"lhs\":{\n" +
            "                                 \"Event\":\"i\"\n" +
            "                              },\n" +
            "                              \"rhs\":{\n" +
            "                                 \"Integer\":1\n" +
            "                              }\n" +
            "                           }\n" +
            "                        },\n" +
            "                        \"rhs\":{\n" +
            "                           \"GreaterThanExpression\":{\n" +
            "                              \"lhs\":{\n" +
            "                                 \"Event\":\"i\"\n" +
            "                              },\n" +
            "                              \"rhs\":{\n" +
            "                                 \"Integer\":3\n" +
            "                              }\n" +
            "                           }\n" +
            "                        }\n" +
            "                     }\n" +
            "                  }\n" +
            "               ]\n" +
            "            }\n" +
            "         }\n" +
            "      }\n" +
            "   ]\n" +
            "}";

    @Test
    public void testOr() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON3);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":0 }" );
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":2 }" );
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":4 }" );
        assertEquals( 1, matchedRules.size() );
    }

    @Test
    public void testAny() {
        RulesSet rulesSet = new RulesSet();
        rulesSet.addRule().withCondition().any()
                .addSingleCondition(prototypeField("i"), Index.ConstraintType.EQUAL, fixedValue(0)).withPatternBinding("event")
                .addSingleCondition(prototypeField("i"), Index.ConstraintType.EQUAL, fixedValue(1)).withPatternBinding("event");

        RulesExecutor rulesExecutor = RulesExecutorFactory.createRulesExecutor(rulesSet);
        checkAnyExecution(rulesExecutor);
    }

    private static final String JSON4 =
            "{\n" +
            "   \"rules\":[\n" +
            "      {\n" +
            "         \"Rule\":{\n" +
            "            \"action\":{\n" +
            "               \"Action\":{\n" +
            "                  \"action\":\"debug\",\n" +
            "                  \"action_args\":{\n" +
            "                     \n" +
            "                  }\n" +
            "               }\n" +
            "            },\n" +
            "            \"condition\":{\n" +
            "               \"AnyCondition\":[\n" +
            "                  {\n" +
            "                     \"AssignmentExpression\":{\n" +
            "                        \"lhs\":{\n" +
            "                           \"Events\":\"event\"\n" +
            "                        },\n" +
            "                        \"rhs\":{\n" +
            "                           \"EqualsExpression\":{\n" +
            "                              \"lhs\":{\n" +
            "                                 \"Event\":\"i\"\n" +
            "                              },\n" +
            "                              \"rhs\":{\n" +
            "                                 \"Integer\":0\n" +
            "                              }\n" +
            "                           }\n" +
            "                        }\n" +
            "                     }\n" +
            "                  },\n" +
            "                  {\n" +
            "                     \"AssignmentExpression\":{\n" +
            "                        \"lhs\":{\n" +
            "                           \"Events\":\"event\"\n" +
            "                        },\n" +
            "                        \"rhs\":{\n" +
            "                           \"EqualsExpression\":{\n" +
            "                              \"lhs\":{\n" +
            "                                 \"Event\":\"i\"\n" +
            "                              },\n" +
            "                              \"rhs\":{\n" +
            "                                 \"Integer\":1\n" +
            "                              }\n" +
            "                           }\n" +
            "                        }\n" +
            "                     }\n" +
            "                  }\n" +
            "               ]\n" +
            "            }\n" +
            "         }\n" +
            "      }\n" +
            "   ]\n" +
            "}";

    @Test
    public void testAnyWithJson() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON4);
        checkAnyExecution(rulesExecutor);
    }

    private void checkAnyExecution(RulesExecutor rulesExecutor) {
        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\": 2 }" );
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\": 1 }" );
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );
        assertEquals( "event", matchedRules.get(0).getDeclarationIds().get(0) );

        matchedRules = rulesExecutor.processFacts( "{ \"i\": 0 }" );
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );
        assertEquals( "event", matchedRules.get(0).getDeclarationIds().get(0) );

        rulesExecutor.dispose();
    }
}