package org.drools.ansible.rulebook.integration.api;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.model.Index;
import org.drools.model.PrototypeFact;
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

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ facts: [ { \"sensu\": { \"data\": { \"i\":3 } } }, { \"j\":3 } ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":4 } } }" ).join();
        assertEquals( 1, matchedRules.size() );

        RuleMatch ruleMatch = RuleMatch.from( matchedRules.get(0) );
        assertEquals( "R3", ruleMatch.getRuleName() );
        assertEquals( 3, ((Map) ruleMatch.getFact("m_3")).get("j") );

        assertEquals( 4, ((Map) ((Map) ((Map) ruleMatch.getFact("m_2")).get("sensu")).get("data")).get("i") );

        rulesExecutor.dispose();
    }

    @Test
    public void testMultipleConditionOnSameFact() {
        String JSON2 =
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON2);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":1 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":3 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":4 }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testOr() {
        String json =
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":0 }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":4 }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testNegate() {
        String json =
                "{\n" +
                "   \"rules\": [\n" +
                "                {\n" +
                "                    \"Rule\": {\n" +
                "                        \"name\": \"r1\",\n" +
                "                        \"condition\": {\n" +
                "                            \"AllCondition\": [\n" +
                "                                {\n" +
                "                                    \"NegateExpression\": {\n" +
                "                                        \"AndExpression\": {\n" +
                "                                            \"lhs\": {\n" +
                "                                                \"GreaterThanExpression\": {\n" +
                "                                                    \"lhs\": {\n" +
                "                                                        \"Event\": \"i\"\n" +
                "                                                    },\n" +
                "                                                    \"rhs\": {\n" +
                "                                                        \"Integer\": 4\n" +
                "                                                    }\n" +
                "                                                }\n" +
                "                                            },\n" +
                "                                            \"rhs\": {\n" +
                "                                                \"LessThanExpression\": {\n" +
                "                                                    \"lhs\": {\n" +
                "                                                        \"Event\": \"i\"\n" +
                "                                                    },\n" +
                "                                                    \"rhs\": {\n" +
                "                                                        \"Integer\": 10\n" +
                "                                                    }\n" +
                "                                                }\n" +
                "                                            }\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            ]\n" +
                "                        },\n" +
                "                        \"action\": {\n" +
                "                            \"Action\": {\n" +
                "                                \"action\": \"print_event\",\n" +
                "                                \"action_args\": {}\n" +
                "                            }\n" +
                "                        },\n" +
                "                        \"enabled\": true\n" +
                "                    }\n" +
                "                }\n" +
                "            ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":7 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":2 }" ).join();
        assertEquals( 1, matchedRules.size() );

        Match match = matchedRules.get(0);
        assertEquals( "r1", match.getRule().getName() );
        assertEquals( 2, ((PrototypeFact)match.getDeclarationValue("m")).get("i") );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":14 }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
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

    @Test
    public void testAnyWithJson() {
        String JSON4 =
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON4);
        checkAnyExecution(rulesExecutor);

        rulesExecutor.dispose();
    }

    private void checkAnyExecution(RulesExecutor rulesExecutor) {
        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\": 2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\": 1 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );
        assertEquals( "event", matchedRules.get(0).getDeclarationIds().get(0) );

        matchedRules = rulesExecutor.processFacts( "{ \"i\": 0 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );
        assertEquals( "event", matchedRules.get(0).getDeclarationIds().get(0) );

        rulesExecutor.dispose();
    }

    @Test
    public void testOrWithNestedAnd() {
        String JSON =
                "{\n" +
                "   \"rules\": [\n" +
                "                {\n" +
                "                    \"Rule\": {\n" +
                "                        \"name\": \"r1\",\n" +
                "                        \"condition\": {\n" +
                "                            \"AllCondition\": [\n" +
                "                                {\n" +
                "                                    \"OrExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"AndExpression\": {\n" +
                "                                                \"lhs\": {\n" +
                "                                                    \"GreaterThanExpression\": {\n" +
                "                                                        \"lhs\": {\n" +
                "                                                            \"Event\": \"i\"\n" +
                "                                                        },\n" +
                "                                                        \"rhs\": {\n" +
                "                                                            \"Integer\": 2\n" +
                "                                                        }\n" +
                "                                                    }\n" +
                "                                                },\n" +
                "                                                \"rhs\": {\n" +
                "                                                    \"LessThanExpression\": {\n" +
                "                                                        \"lhs\": {\n" +
                "                                                            \"Event\": \"i\"\n" +
                "                                                        },\n" +
                "                                                        \"rhs\": {\n" +
                "                                                            \"Integer\": 4\n" +
                "                                                        }\n" +
                "                                                    }\n" +
                "                                                }\n" +
                "                                            }\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"AndExpression\": {\n" +
                "                                                \"lhs\": {\n" +
                "                                                    \"LessThanExpression\": {\n" +
                "                                                        \"lhs\": {\n" +
                "                                                            \"Event\": \"i\"\n" +
                "                                                        },\n" +
                "                                                        \"rhs\": {\n" +
                "                                                            \"Integer\": 8\n" +
                "                                                        }\n" +
                "                                                    }\n" +
                "                                                },\n" +
                "                                                \"rhs\": {\n" +
                "                                                    \"GreaterThanExpression\": {\n" +
                "                                                        \"lhs\": {\n" +
                "                                                            \"Event\": \"i\"\n" +
                "                                                        },\n" +
                "                                                        \"rhs\": {\n" +
                "                                                            \"Integer\": 6\n" +
                "                                                        }\n" +
                "                                                    }\n" +
                "                                                }\n" +
                "                                            }\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            ]\n" +
                "                        },\n" +
                "                        \"action\": {\n" +
                "                            \"Action\": {\n" +
                "                                \"action\": \"print_event\",\n" +
                "                                \"action_args\": {}\n" +
                "                            }\n" +
                "                        },\n" +
                "                        \"enabled\": true\n" +
                "                    }\n" +
                "                }\n" +
                "            ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\":0 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":5 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\":7 }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testPreventSelfJoin() {
        String json =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"name\":\"R1\",\n" +
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
                "      }\n" +
                "   ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"data\": { \"i\":3 } }, \"j\":2 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"sensu\": { \"data\": { \"i\":3 } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"j\":2 }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testComparisonWithDifferentNumericTypes() {
        String json =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"echo\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"AndExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"EqualsExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"action\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"String\": \"go\"\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"GreaterThanExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"i\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"Float\": 1.5\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                },\n" +
                "                \"actions\": [\n" +
                "                    {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"print_event\",\n" +
                "                            \"action_args\": {}\n" +
                "                        }\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\":3, \"action\":\"go\" }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testEqualityWithDifferentNumericTypes() {
        String json =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"echo\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"AndExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"EqualsExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"action\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"String\": \"go\"\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"EqualsExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"i\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"Float\": 3.0\n" + // 3.0 becomes BigDecimal("3.0") by ConditionParseUtil.toJsonValue()
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                },\n" +
                "                \"actions\": [\n" +
                "                    {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"print_event\",\n" +
                "                            \"action_args\": {}\n" +
                "                        }\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\":3, \"action\":\"go\" }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"i\":3.00, \"action\":\"go\" }" ).join(); // 3.00 becomes BigDecimal("3.00") by RulesModelUtil.asFactMap()
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testOrWithAnd() {
        String json =
                "{\n" +
                "    \"rules\": [\n" +
                "        {\n" +
                "            \"Rule\": {\n" +
                "                \"name\": \"Test and-or operator simple\",\n" +
                "                \"condition\": {\n" +
                "                    \"AllCondition\": [\n" +
                "                        {\n" +
                "                            \"AndExpression\": {\n" +
                "                                \"lhs\": {\n" +
                "                                    \"OrExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"EqualsExpression\": {\n" +
                "                                                \"lhs\": {\n" +
                "                                                    \"Event\": \"myint\"\n" +
                "                                                },\n" +
                "                                                \"rhs\": {\n" +
                "                                                    \"Integer\": 73\n" +
                "                                                }\n" +
                "                                            }\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"EqualsExpression\": {\n" +
                "                                                \"lhs\": {\n" +
                "                                                    \"Event\": \"mystring\"\n" +
                "                                                },\n" +
                "                                                \"rhs\": {\n" +
                "                                                    \"String\": \"world\"\n" +
                "                                                }\n" +
                "                                            }\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                },\n" +
                "                                \"rhs\": {\n" +
                "                                    \"EqualsExpression\": {\n" +
                "                                        \"lhs\": {\n" +
                "                                            \"Event\": \"mystring\"\n" +
                "                                        },\n" +
                "                                        \"rhs\": {\n" +
                "                                            \"String\": \"hello\"\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                },\n" +
                "                \"actions\": [\n" +
                "                    {\n" +
                "                        \"Action\": {\n" +
                "                            \"action\": \"echo\",\n" +
                "                            \"action_args\": {\n" +
                "                                \"message\": \"Test and-or operator #1 passes\"\n" +
                "                            }\n" +
                "                        }\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"enabled\": true\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n\n" +
        "}";

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"id\": \"test_and_operator\", \"myint\": 73, \"mystring\": \"hello\" }" ).join();
        assertEquals( 1, matchedRules.size() );

        matchedRules = rulesExecutor.processEvents( "{ \"id\": \"test_and_operator\", \"myint\": 73, \"mystring\": \"world\" }" ).join();
        assertEquals( 0, matchedRules.size() );

        rulesExecutor.dispose();
    }
}