package org.drools.ansible.rulebook.integration.api;

import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SessionStatsTest {

    @Test
    public void testWithDisabledRule() {
        String json =
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
                      "enabled": false,
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

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"j\":1 }" ).join();
        assertEquals( 0, matchedRules.size() );

        SessionStats stats = rulesExecutor.getSessionStats();
        assertEquals( 3, stats.getNumberOfRules() );
        assertEquals( 1, stats.getNumberOfDisabledRules() );
        assertEquals( 1, stats.getRulesTriggered() );
        assertEquals( 1, stats.getPermanentStorageCount() );
        assertTrue( stats.getPermanentStorageSize() > 10 );
        assertTrue( stats.getPermanentStorageSize() < 1000 );
        assertEquals( "R1", stats.getLastRuleFired() );

        rulesExecutor.dispose();
    }
}
