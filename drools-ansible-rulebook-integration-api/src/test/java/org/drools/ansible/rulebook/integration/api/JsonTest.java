package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.api.ObjectMapperFactory.createMapper;
import static org.junit.Assert.assertEquals;

public class JsonTest {

    public static final String JSON1 =
            """
            {
               "sources":{"EventSource":"test"},
               "rules":[
                  {"Rule": {
                     "condition":{
                       "AssignmentExpression": {
                         "lhs": {
                           "Events": "first"
                         },
                         "rhs": {
                           "EqualsExpression":{
                             "lhs":{
                                "Event":"sensu.data.i"
                             },
                             "rhs":{
                                "Integer":1
                             }
                           }
                         }
                       }
                     },
                     "action":{
                        "assert_fact":{
                           "ruleset":"Test rules4",
                           "fact":{
                              "j":1
                           }
                        }
                     }
                  }},
                  {"Rule": {
                     "condition":{
                        "EqualsExpression":{
                           "lhs":{
                              "sensu":"data.i"
                           },
                           "rhs":{
                              "Integer":2
                           }
                        }
                     },
                     "action":{
                        "run_playbook":[
                           {
                              "name":"hello_playbook.yml"
                           }
                        ]
                     }
                  }},
                  {"Rule": {
                     "condition":{
                        "EqualsExpression":{
                           "lhs":{
                              "sensu":"data.i"
                           },
                           "rhs":{
                              "Integer":3
                           }
                        }
                     },
                     "action":{
                        "retract_fact":{
                           "ruleset":"Test rules4",
                           "fact":{
                              "j":3
                           }
                        }
                     }
                  }},
                  {"Rule": {
                     "condition":{
                        "AllCondition":[{
                          "EqualsExpression":{
                             "lhs":"j",
                             "rhs":{
                                "Integer":1
                             }
                          }
                        }]\
                      },
                     "action":{
                        "post_event":{
                           "ruleset":"Test rules4",
                           "fact":{
                              "j":4
                           }
                        }
                     }
                  }}
               ]
            }
            """;

    @Test
    public void testReadJson() throws JsonProcessingException {
        System.out.println(JSON1);

        ObjectMapper mapper = createMapper(new JsonFactory());
        RulesSet rulesSet = mapper.readValue(JSON1, RulesSet.class);
        System.out.println(rulesSet);
    }

    @Test
    public void testExecuteRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);
        int executedRules = rulesExecutor.executeFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 2, executedRules );
        rulesExecutor.dispose();
    }

    @Test
    public void testProcessRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );
        assertEquals( 1, matchedRules.get(0).getDeclarationIds().size() );
        assertEquals( "first", matchedRules.get(0).getDeclarationIds().get(0) );

        matchedRules = rulesExecutor.processFacts( "{ \"j\":1 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_3", matchedRules.get(0).getRule().getName() );
        assertEquals( 1, matchedRules.get(0).getDeclarationIds().size() );
        assertEquals( "m", matchedRules.get(0).getDeclarationIds().get(0) );

        rulesExecutor.dispose();
    }

    @Test
    public void testProcessRuleWithoutAction() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson("{ \"rules\": [ {\"Rule\": { \"name\": \"R1\", \"condition\":{ \"EqualsExpression\":{ \"lhs\":{ \"sensu\":\"data.i\" }, \"rhs\":{ \"Integer\":1 } } } }} ] }");

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testProcessNumericString() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson("{ \"rules\": [ {\"Rule\": { \"name\": \"R1\", \"condition\":{ \"EqualsExpression\":{ \"lhs\":{ \"sensu\":\"data.i\" }, \"rhs\":{ \"String\":\"1\" } } } }} ] }");

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":\"1\" } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testProcessRuleWithUnknownAction() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson("{ \"rules\": [ {\"Rule\": { \"name\": \"R1\", \"condition\":{ \"EqualsExpression\":{ \"lhs\":{ \"sensu\":\"data.i\" }, \"rhs\":{ \"Integer\":1 } } }, \"action\": { \"unknown\": { \"ruleset\": \"Test rules4\", \"fact\": { \"j\": 1 } } } }} ] }\n");

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testProcessRuleIgnoringActionsTag() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson("{ \"rules\": [ {\"Rule\": { \"name\": \"R1\", \"condition\":{ \"EqualsExpression\":{ \"lhs\":{ \"sensu\":\"data.i\" }, \"rhs\":{ \"Integer\":1 } } }, \"actions\": { \"post_event\": { \"ruleset\": \"Test rules4\", \"fact\": { \"j\": 1 } } } }} ] }\n");

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testIsDefinedExpression() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson("{ \"rules\": [ {\"Rule\": { \"name\": \"R1\", \"condition\":{ \"IsDefinedExpression\":{ \"sensu\":\"data.i\" } } }} ] }");

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testIsDefinedExpressionOnMap() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson("{ \"rules\": [ {\"Rule\": { \"name\": \"R1\", \"condition\":{ \"IsDefinedExpression\":{ \"event\":\"payload\" } } }} ] }");

        List<Match> matchedRules = rulesExecutor.processFacts( "{\"payload\": {\"key1\": \"value1\", \"key2\": \"value2\"}}" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    public void testProcessNoteEqualsWithNull() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson("{ \"rules\": [ {\"Rule\": { \"name\": \"R1\", \"condition\":{ \"NotEqualsExpression\":{ \"lhs\":{ \"sensu\":\"data.i\" }, \"rhs\":{ \"Integer\":1 } } } }} ] }");

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"j\":1 } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":2 } } }" ).join();
        assertEquals( 1, matchedRules.size() );

        rulesExecutor.dispose();
    }

    @Test
    public void testProcessRuleWithFloat() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson("{ \"rules\": [ {\"Rule\": { \"name\": \"R1\", \"condition\":{ \"LessThanExpression\":{ \"lhs\":{ \"Event\": \"i\" }, \"rhs\":{ \"Float\": 200.89 } } } }} ] }");

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"i\": 200.9 }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"i\": 200.8 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }
}
