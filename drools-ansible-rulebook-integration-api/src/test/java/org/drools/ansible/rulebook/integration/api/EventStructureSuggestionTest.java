package org.drools.ansible.rulebook.integration.api;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.rulesengine.RuleEngineTestUtils.disableEventStructureSuggestion;
import static org.drools.ansible.rulebook.integration.api.rulesengine.RuleEngineTestUtils.enableEventStructureSuggestion;
import static org.junit.Assert.assertEquals;

public class EventStructureSuggestionTest {

    // For logging assertion
    static PrintStream originalOut = System.out;
    static StringPrintStream stringPrintStream = new StringPrintStream(System.out);

    @BeforeClass
    public static void beforeClass() {
        enableEventStructureSuggestion();
        System.setOut(stringPrintStream);
    }

    @AfterClass
    public static void afterClass() {
        disableEventStructureSuggestion();
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

        assertNumberOfRulesSetEventStructureWarnLogs(0);
        assertEquals(1, matchedRules.size());

        rulesExecutor.dispose();
    }

    private static void assertNumberOfRulesSetEventStructureWarnLogs(int expected) {
        assertThat(stringPrintStream.getStringList().stream().filter(s -> s.contains("WARN") && s.contains("RulesSetEventStructure")).count()).isEqualTo(expected);
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

        assertNumberOfRulesSetEventStructureWarnLogs(1);
        assertThat(stringPrintStream.getStringList())
                .anyMatch(s -> s.contains("'alerts[]' in the condition 'alerts[].labels.job'" +
                                                  " in rule set 'ruleSet1' rule [r1]" +
                                                  " does not meet with the incoming event property [payload]." +
                                                  " Did you forget to include 'payload'?"));

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

        List<Match> matchedRules = rulesExecutor.processEvents(EVENT).join();

        assertNumberOfRulesSetEventStructureWarnLogs(1);
        assertThat(stringPrintStream.getStringList())
                .anyMatch(s -> s.contains("'lebel' in the condition 'payload.alerts[].lebel.job'" +
                                                  " in rule set 'ruleSet1' rule [r1]" +
                                                  " does not meet with the incoming event property [labels]." +
                                                  " Did you mean 'labels'?"));
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    public static final String JSON_GITHUB =
            """
                    {
                        "name": "ruleSet1",
                        "rules": [
                             {
                                 "Rule": {
                                     "name": "r1",
                                     "condition": {
                                         "AnyCondition": [
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "events[0]"
                                                     },
                                                     "rhs": {
                                                         "String": "push"
                                                     }
                                                 }
                                             },
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "repositry.fork"
                                                     },
                                                     "rhs": {
                                                         "Boolean": "true"
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
    public void githubEvent() throws IOException {
        // A rule condition misses the "hook" node
        // and another condition has a typo "repository" -> "repositry"
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_GITHUB);

        Path path = Paths.get("src/test/resources/github-event.json");
        String githubEvent = Files.readString(path);

        List<Match> matchedRules = rulesExecutor.processEvents(githubEvent).join();

        assertNumberOfRulesSetEventStructureWarnLogs(2);
        assertThat(stringPrintStream.getStringList())
                .anyMatch(s -> s.contains("'events[]' in the condition 'events[]'" +
                                                  " in rule set 'ruleSet1' rule [r1]" +
                                                  " does not meet with the incoming event property" +
                                                  " [zen, hook, sender, hook_id, repository]." +
                                                  " Did you forget to include 'hook'?"))
                .anyMatch(s -> s.contains("'repositry' in the condition 'repositry.fork'" +
                                                  " in rule set 'ruleSet1' rule [r1]" +
                                                  " does not meet with the incoming event property" +
                                                  " [zen, hook, sender, hook_id, repository]." +
                                                  " Did you mean 'repository'?"));
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }


    public static final String JSON_GITLAB =
            """
                    {
                        "name": "ruleSet1",
                        "rules": [
                             {
                                 "Rule": {
                                     "name": "r1",
                                     "condition": {
                                         "AnyCondition": [
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "commit[0].added[1]"
                                                     },
                                                     "rhs": {
                                                         "String": "extensions/eda/rulebooks/100_million_events.yml"
                                                     }
                                                 }
                                             },
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "added[0]"
                                                     },
                                                     "rhs": {
                                                         "String": "rulebooks/limits_with_vault_secret.yml"
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
    public void gitlabEvent() throws IOException {
        // A rule condition has a typo "commits[]" -> "commits" (forgetting [] access is the same as typo)
        // and another condition misses the "commits[]" node
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_GITLAB);

        Path path = Paths.get("src/test/resources/gitlab-event.json");
        String gitlabEvent = Files.readString(path);

        List<Match> matchedRules = rulesExecutor.processEvents(gitlabEvent).join();

        assertNumberOfRulesSetEventStructureWarnLogs(2);
        assertThat(stringPrintStream.getStringList())
                .anyMatch(s -> s.contains("'commit[]' in the condition 'commit[].added[]'" +
                                                  " in rule set 'ruleSet1' rule [r1]" +
                                                  " does not meet with the incoming event property" +
                                                  " [commits[], user_avatar, total_commits_count, user_email, before," +
                                                  " push_options, user_name, user_username, checkout_sha, project," +
                                                  " ref_protected, message, repository, object_kind, ref, project_id," +
                                                  " user_id, event_name, after]." +
                                                  " Did you mean 'commits[]'?"))
                .anyMatch(s -> s.contains("'added[]' in the condition 'added[]'" +
                                                  " in rule set 'ruleSet1' rule [r1]" +
                                                  " does not meet with the incoming event property" +
                                                  " [commits[], user_avatar, total_commits_count, user_email, before," +
                                                  " push_options, user_name, user_username, checkout_sha, project," +
                                                  " ref_protected, message, repository, object_kind, ref, project_id," +
                                                  " user_id, event_name, after]." +
                                                  " Did you forget to include 'commits[]'?"));
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    public static final String JSON_TYPO_MULTI =
            """
                    {
                        "name": "ruleSet1",
                        "rules": [
                             {
                                 "Rule": {
                                     "name": "r1",
                                     "condition": {
                                         "AnyCondition": [
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "payload.alerts[0].lebel.job"
                                                     },
                                                     "rhs": {
                                                         "String": "kube-state-metrics"
                                                     }
                                                 }
                                             },
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "payload.alerts[0].lebel.job"
                                                     },
                                                     "rhs": {
                                                         "String": "node-exporter"
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
                             },
                             {
                                 "Rule": {
                                     "name": "r2",
                                     "condition": {
                                         "AllCondition": [
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "payload.alerts[0].lebel.job"
                                                     },
                                                     "rhs": {
                                                         "String": "kubelet-stats"
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
    public void sameTypos() {
        // The rules have the same typo on 3 conditions
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_TYPO_MULTI);

        List<Match> matchedRules = rulesExecutor.processEvents(EVENT).join();

        assertNumberOfRulesSetEventStructureWarnLogs(1);
        assertThat(stringPrintStream.getStringList())
                .anyMatch(s -> s.contains("'lebel' in the condition 'payload.alerts[].lebel.job'" +
                                                  " in rule set 'ruleSet1' rule [r1, r1, r2]" +
                                                  " does not meet with the incoming event property [labels]." +
                                                  " Did you mean 'labels'?"));
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    public static final String JSON_TYPO_MATCH =
            """
                    {
                        "name": "ruleSet1",
                        "rules": [
                             {
                                 "Rule": {
                                     "name": "r1",
                                     "condition": {
                                         "AnyCondition": [
                                             {
                                                 "EqualsExpression": {
                                                     "lhs": {
                                                         "Event": "payload.alerts[0].lebel.job"
                                                     },
                                                     "rhs": {
                                                         "String": "node-exporter"
                                                     }
                                                 }
                                             },
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
    public void typoButMatch_shouldNotLogWarn() {
        // One condition has a typo labels -> lebel
        // , but the other condition matches, so the rule matches
        // Validation doesn't take place
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_TYPO_MATCH);

        List<Match> matchedRules = rulesExecutor.processEvents(EVENT).join();

        assertNumberOfRulesSetEventStructureWarnLogs(0);
        assertEquals(1, matchedRules.size());

        rulesExecutor.dispose();
    }

    public static final String EVENT_NO_MATCH =
            """
                    {
                      "payload":{
                        "alerts":[
                          {
                            "labels":{
                              "job":"XXX"
                            }
                          }
                        ]
                      }
                    }
                    """;

    @Test
    public void typoButMatchThenNoMatch_shouldNotLogWarn() {
        // One condition has a typo labels -> lebel
        // , but the other condition matches, so the rule matches
        // Validation doesn't take place
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_TYPO_MATCH);

        List<Match> matchedRules = rulesExecutor.processEvents(EVENT).join();

        assertNumberOfRulesSetEventStructureWarnLogs(0);
        assertEquals(1, matchedRules.size());

        // Then, next event doesn't match
        matchedRules = rulesExecutor.processEvents(EVENT_NO_MATCH).join();

        // Still no warn log, because it's not the first event
        assertNumberOfRulesSetEventStructureWarnLogs(0);
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    public void factThenEvent() {
        // One condition has a typo
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_TYPO);

        // insert a fact (same json as EVENT, though). No match
        List<Match> matchedRules = rulesExecutor.processFacts(EVENT).join();

        // no warn log, because it's not the first event
        assertNumberOfRulesSetEventStructureWarnLogs(0);
        assertEquals(0, matchedRules.size());

        // Then, insert an event. No match
        matchedRules = rulesExecutor.processEvents(EVENT).join();

        // Warn log, because it's the first event
        assertNumberOfRulesSetEventStructureWarnLogs(1);
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }


    public static final String JSON_NESTED_ARRAY =
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
                                                         "Event": "payload.alerts[0][0].jobjob"
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
    public void nestedArray() {
        // The rule condition has a nested array "alerts[0][0]"
        // Validation doesn't analyze nested arrays (limitation) and its children, so no waring is logged
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_NESTED_ARRAY);

        List<Match> matchedRules = rulesExecutor.processEvents(EVENT).join();

        assertNumberOfRulesSetEventStructureWarnLogs(0);
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    public static final String JSON_WHITESPACE =
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
                                                         "Event": " payload .alerts[0]  .lebel . job"
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
    public void whitespace() {
        // whitespaces in the event path are trimmed, so it doesn't affect the validation
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_WHITESPACE);

        List<Match> matchedRules = rulesExecutor.processEvents(EVENT).join();

        assertNumberOfRulesSetEventStructureWarnLogs(1);
        assertThat(stringPrintStream.getStringList())
                .anyMatch(s -> s.contains("'lebel' in the condition 'payload.alerts[].lebel.job'" +
                                                  " in rule set 'ruleSet1' rule [r1]" +
                                                  " does not meet with the incoming event property [labels]." +
                                                  " Did you mean 'labels'?"));
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    public static final String EVENTS =
            """
                {
                  "events": [
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
                    },
                    {
                      "payload":{
                        "alerts":[
                          {
                            "labels":{
                              "job":"node-exporter"
                            }
                          }
                        ]
                      }
                    }
                  ]
                }
                    """;

    @Test
    public void events() {

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_TYPO);

        // "events" is the root node of multiple events, so the first event is used for the validation
        List<Match> matchedRules = rulesExecutor.processEvents(EVENTS).join();

        assertNumberOfRulesSetEventStructureWarnLogs(1);
        assertThat(stringPrintStream.getStringList())
                .anyMatch(s -> s.contains("'lebel' in the condition 'payload.alerts[].lebel.job'" +
                                                  " in rule set 'ruleSet1' rule [r1]" +
                                                  " does not meet with the incoming event property [labels]." +
                                                  " Did you mean 'labels'?"));
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }

    @Test
    public void arrayWithoutBracket() {
        String JSON_ARRAY_IN_ARRAY =
                """
                {
                  "rules":[
                    {
                      "Rule":{
                        "name":"r1",
                        "condition":{
                          "AllCondition":[
                            {
                              "SelectAttrExpression":{
                                "lhs":{
                                  "Event":"incident.alerts.tags"
                                },
                                "rhs":{
                                  "key":{
                                    "String":"value"
                                  },
                                  "operator":{
                                    "String":"=="
                                  },
                                  "value":{
                                    "String":"XXXX"
                                  }
                                }
                              }
                            }
                          ]
                        },
                        "actions":[
                          {
                            "Action":{
                              "action":"debug",
                              "action_args":{
                                "msg":"Found a match with alerts"
                              }
                            }
                          }
                        ],
                        "enabled":true
                      }
                    }
                  ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_ARRAY_IN_ARRAY);

        List<Match> matchedRules = rulesExecutor.processEvents( """
                {
                  "incident":{
                    "id":"aaa",
                    "active":false,
                    "alerts":[
                      {
                        "id":"bbb",
                        "tags":[
                          {
                            "name":"alertname",
                            "value":"MariadbDown"
                          },
                          {
                            "name":"severity",
                            "value":"critical"
                          }
                        ],
                        "status":"Ok"
                      },
                      {
                        "id":"ccc",
                        "tags":[
                          {
                            "name":"severity",
                            "value":"critical"
                          },
                          {
                            "name":"alertname",
                            "value":"DiskUsage"
                          }
                        ],
                        "status":"Ok"
                      }
                    ]
                  }
                }
                """ ).join();

        // "alerts" and "tags" without brackets are accepted. No warning. See DROOLS-7639
        assertNumberOfRulesSetEventStructureWarnLogs(0);
        assertEquals(0, matchedRules.size());

        rulesExecutor.dispose();
    }
}