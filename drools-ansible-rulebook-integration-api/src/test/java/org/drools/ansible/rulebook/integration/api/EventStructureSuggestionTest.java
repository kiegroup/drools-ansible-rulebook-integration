package org.drools.ansible.rulebook.integration.api;

import java.io.PrintStream;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class EventStructureSuggestionTest {

    // For logging assertion
    static PrintStream originalOut = System.out;
    static StringPrintStream stringPrintStream = new StringPrintStream(System.out);

    @BeforeClass
    public static void beforeClass() {
        System.setOut(stringPrintStream);
    }

    @AfterClass
    public static void afterClass() {
        System.setOut(originalOut);
    }

    @Before
    public void before() {
        stringPrintStream.getStringList().clear();
    }

    // Assume that the incoming event has the correct structure
    public static final String EVENT =
            """
                    {
                      "payload":{
                        "alerts":[
                          {
                            "labels":{
                              "job":"kube-state-metrics"
                            }
                          }
                        ]
                      }
                    }
                    """;

    public static final String JSON_VALID =
            """
                    {
                        "name": "ruleSet1",
                        "rules": [
                             {
                                 "Rule": {
                                     "name": "r1",
                                     "condition": {
                                         "AllCondition": [
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "payload.alerts[0].labels.job"
                                                     },
                                                     "rhs": {
                                                         "String": "kube-state-metrics"
                                                     }
                                                 }
                                             }
                                         ]
                                     },
                                     "actions": [
                                         {
                                             "Action": {
                                                 "action": "debug",
                                                 "action_args": {}
                                             }
                                         }
                                     ],
                                     "enabled": true
                                 }
                             }
                         ]
                    }
                    """;

    @Test
    public void validEventPath() {
        // The rule is valid, so no suggestion is logged
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_VALID);

        List<Match> matchedRules = rulesExecutor.processEvents(EVENT).join();

        // TODO: assert no warn log
        assertEquals(1, matchedRules.size());

        rulesExecutor.dispose();
    }

    public static final String JSON_MISSING_FIRST_NODE =
            """
                    {
                        "name": "ruleSet1",
                        "rules": [
                             {
                                 "Rule": {
                                     "name": "r1",
                                     "condition": {
                                         "AllCondition": [
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "alerts[0].labels.job"
                                                     },
                                                     "rhs": {
                                                         "String": "kube-state-metrics"
                                                     }
                                                 }
                                             }
                                         ]
                                     },
                                     "actions": [
                                         {
                                             "Action": {
                                                 "action": "debug",
                                                 "action_args": {}
                                             }
                                         }
                                     ],
                                     "enabled": true
                                 }
                             }
                         ]
                    }
                    """;

    @Test
    public void missingFirstNode() {
        // The rule condition misses the "payload" node
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_MISSING_FIRST_NODE);

        List<Match> matchedRules = rulesExecutor.processEvents(EVENT).join();

        // TODO: assert warn log
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    public static final String JSON_TYPO =
            """
                    {
                        "name": "ruleSet1",
                        "rules": [
                             {
                                 "Rule": {
                                     "name": "r1",
                                     "condition": {
                                         "AllCondition": [
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "payload.alerts[0].lebel.job"
                                                     },
                                                     "rhs": {
                                                         "String": "kube-state-metrics"
                                                     }
                                                 }
                                             }
                                         ]
                                     },
                                     "actions": [
                                         {
                                             "Action": {
                                                 "action": "debug",
                                                 "action_args": {}
                                             }
                                         }
                                     ],
                                     "enabled": true
                                 }
                             }
                         ]
                    }
                    """;

    @Test
    public void typo() {
        // The rule has a typo labels -> lebel
        // 2 characters difference can be detected at most
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_TYPO);

        // The rule condition misses the "payload" node
        List<Match> matchedRules = rulesExecutor.processEvents(EVENT).join();

        // TODO: assert warn log
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }
}