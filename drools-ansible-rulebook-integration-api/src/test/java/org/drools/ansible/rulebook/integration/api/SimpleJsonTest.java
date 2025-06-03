package org.drools.ansible.rulebook.integration.api;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;

import static org.drools.ansible.rulebook.integration.api.ObjectMapperFactory.createMapper;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleJsonTest {

    private static final String JSON1 =
            """
            {
              "rules": [
                {"Rule": {
                  "name": "R1",
                  "condition": "sensu.data.i == 1",
                  "action": {
                    "assert_fact": {
                      "ruleset": "Test rules4",
                      "fact": {
                        "j": 1
                      }
                    }
                  }
                }},
                {"Rule": {
                  "name": "R2",
                  "condition": "sensu.data.i == 2",
                  "action": {
                    "run_playbook": [
                      {
                        "name": "hello_playbook.yml"
                      }
                    ]
                  }
                }},
                {"Rule": {
                  "name": "R3",
                  "condition": "sensu.data.i == 3",
                  "action": {
                    "retract_fact": {
                      "ruleset": "Test rules4",
                      "fact": {
                        "j": 3
                      }
                    }
                  }
                }},
                {"Rule": {
                  "name": "R4",
                  "condition": "j == 1",
                  "action": {
                    "post_event": {
                      "ruleset": "Test rules4",
                      "fact": {
                        "j": 4
                      }
                    }
                  }
                }}
              ]
            }
            """;

    @Test
    void testReadJson() throws JsonProcessingException {
        ObjectMapper mapper = createMapper(new JsonFactory());
        RulesSet rulesSet = mapper.readValue(JSON1, RulesSet.class);
        System.out.println(rulesSet);
    }

    @Test
    void testExecuteRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);
        int executedRules = rulesExecutor.executeFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 2, executedRules );
        rulesExecutor.dispose();
    }

    @Test
    void testProcessRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"j\":1 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R4", matchedRules.get(0).getRule().getName() );

        SessionStats stats = rulesExecutor.dispose();
        assertEquals( 4, stats.getNumberOfRules() );
        assertEquals( 0, stats.getNumberOfDisabledRules() );
        assertEquals( 2, stats.getRulesTriggered() );
        assertEquals( 2, stats.getPermanentStorageCount() );
    }

    @Test
    void testProcessRuleWithoutAction() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson("{ \"rules\": [ {\"Rule\": { \"name\": \"R1\", \"condition\": \"sensu.data.i == 1\" }} ] }");

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    @Test
    void testProcessRuleWithUnknownAction() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson("{ \"rules\": [ {\"Rule\": { \"name\": \"R1\", \"condition\": \"sensu.data.i == 1\", \"action\": { \"unknown\": { \"ruleset\": \"Test rules4\", \"fact\": { \"j\": 1 } } } }} ] }\n");

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }
}
