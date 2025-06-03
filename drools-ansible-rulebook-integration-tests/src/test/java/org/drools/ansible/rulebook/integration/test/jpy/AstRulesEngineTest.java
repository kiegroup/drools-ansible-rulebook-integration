package org.drools.ansible.rulebook.integration.test.jpy;

import org.drools.ansible.rulebook.integration.api.JsonTest;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


public class AstRulesEngineTest {
    @Test
    void testJpyApi() {
        String rules = JsonTest.JSON1;
        AstRulesEngine engine = new AstRulesEngine();
        long sessionId = 0;
        try {
            sessionId = engine.createRuleset(rules);
            String result = engine.assertFact(sessionId, "{ \"sensu\": { \"data\": { \"i\":1 } } }");
            assertNotNull(result);
        } finally {
            String sessionStats = engine.sessionStats(sessionId);
            Map<String, Object> statsMap = JsonMapper.readValueAsMapOfStringAndObject(sessionStats);
            assertEquals(4, statsMap.get("numberOfRules"));
            assertEquals(0, statsMap.get("numberOfDisabledRules"));
            assertEquals(1, statsMap.get("rulesTriggered"));
            assertFalse(statsMap.keySet().contains("end"));

            String disposeStats = engine.dispose(sessionId);
            Map<String, Object> disposeStatsMap = JsonMapper.readValueAsMapOfStringAndObject(disposeStats);
            assertTrue(disposeStatsMap.keySet().contains("end"));
        }
    }

    @Test
    void testBrokenApi() throws IOException {
        try (AstRulesEngine engine = new AstRulesEngine();
             InputStream s = getClass().getClassLoader().getResourceAsStream("broken.json")) {
            String rules = new String(s.readAllBytes());
            assertThrows(UnsupportedOperationException.class, () -> engine.createRuleset(rules));
        }
    }

    @Test
    void testRetractFactFullMatch() throws IOException {
        try (AstRulesEngine engine = new AstRulesEngine();
             InputStream s = getClass().getClassLoader().getResourceAsStream("retract_fact.json")) {
            String rules = new String(s.readAllBytes());

            long id = engine.createRuleset(rules);
            String fact1 = "{\"j\": 42}";
            engine.assertFact(id, fact1);
            String fact2 = "{\"i\": 67}";
            engine.assertFact(id, fact2);
            String retractedFact = "{\"i\": 67}";

            String facts = engine.getFacts(id);
            List<Map<String, Object>> factList = JsonMapper.readValueAsListOfMapOfStringAndObject(facts);
            assertEquals(1, factList.size());
            assertEquals(factList.get(0), JsonMapper.readValueAsRawObject(fact2));

            String r = engine.retractMatchingFacts(id, retractedFact, false);

            List<Map<String, Object>> v = JsonMapper.readValueAsListOfMapOfStringAndObject(r);
            assertEquals(1, v.size());
            assertEquals(((Map) v.get(0).get("r_0")).get("m"), JsonMapper.readValueAsRawObject(fact2));
        }
    }

    @Test
    void testRetractMatchingFacts() throws IOException {
        try (AstRulesEngine engine = new AstRulesEngine();
             InputStream s = getClass().getClassLoader().getResourceAsStream("retract_fact.json")) {
            String rules = new String(s.readAllBytes());

            long id = engine.createRuleset(rules);
            String fact1 = "{\"j\": 42, \"y\": 2}";
            engine.assertFact(id, fact1);
            String fact2 = "{\"i\": 67, \"x\": 1}";
            engine.assertFact(id, fact2);
            String retractedFact = "{\"i\": 67}";
            String r = engine.retractMatchingFacts(id, retractedFact, true);

            List<Map<String, Object>> v = JsonMapper.readValueAsListOfMapOfStringAndObject(r);

            assertEquals(((Map) v.get(0).get("r_0")).get("m"), JsonMapper.readValueAsRawObject(fact2));
        }
    }

    @Test
    void testTimedOut() throws IOException {
        try (AstRulesEngine engine = new AstRulesEngine();
             InputStream s = getClass().getClassLoader().getResourceAsStream("timed_out.json")) {
            String rules = new String(s.readAllBytes());
            long id = engine.createRuleset(rules);
            int port = engine.port();

            try (Socket socket = new Socket("localhost", port)) {
                DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream());

                long assertTime = System.nanoTime();
                engine.assertEvent(id, "{\"j\": 42}");

                int l = bufferedInputStream.readInt();
                long elapsed = (System.nanoTime() - assertTime) / 1_000_000;

                // fires after at least 2 seconds
                assertTrue(elapsed >= 1_900, "rule fired after " + elapsed + " milliseconds");

                byte[] bytes = bufferedInputStream.readNBytes(l);
                String r = new String(bytes, StandardCharsets.UTF_8);

                List<Object> matches = JsonMapper.readValueExtractFieldAsList(r, "result");
                Map<String, Map> match = (Map<String, Map>) matches.get(0);

                assertNotNull(match.get("r1"));
            }
        }
    }

    @Test
    void testTimedOutWithAdvance() throws IOException {
        try (AstRulesEngine engine = new AstRulesEngine();
             InputStream s = getClass().getClassLoader().getResourceAsStream("timed_out.json")) {
            int port = engine.port();
            String rules = new String(s.readAllBytes());
            long id = engine.createRuleset(rules);

            try (Socket socket = new Socket("localhost", port)) {
                DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream());

                engine.assertFact(id, "{\"j\": 42}");
                String r = engine.advanceTime(id, 3, "SECONDS");

                List<Map<String, Object>> v = JsonMapper.readValueAsListOfMapOfStringAndObject(r);
                assertNotNull(v.get(0).get("r1"));

                int l = bufferedInputStream.readInt();
                byte[] bytes = bufferedInputStream.readNBytes(l);
                String r2 = new String(bytes, StandardCharsets.UTF_8);

                List<Object> matches2 = JsonMapper.readValueExtractFieldAsList(r2, "result");
                Map<String, Map> match = (Map<String, Map>) matches2.get(0);
                assertNotNull(match.get("r1"));
            }
        }
    }

    @Test
    @Timeout(5000L)
    void testThrowExceptionOnUnboundSocket() throws IOException {
        try (AstRulesEngine engine = new AstRulesEngine();
             InputStream s = getClass().getClassLoader().getResourceAsStream("timed_out.json")) {
            int port = engine.port();
            String rules = new String(s.readAllBytes());
            long id = engine.createRuleset(rules);

            try {
                engine.assertFact(id, "{\"j\": 42}");
                fail("trying to use the engine before having opened a connection should fail");
            } catch (IllegalStateException e) {
                // expected
            }
        }
    }

    @Test
    void testAssertEvent() throws IOException {
        String rules = """
                {
                   "rules":[
                      {
                         "Rule":{
                            "name":"Create Snapshot",
                            "condition":{
                               "AllCondition":[
                                  {
                                     "EqualsExpression":{
                                        "lhs":{
                                           "Event":"type"
                                        },
                                        "rhs":{
                                           "String":"MODIFIED"
                                        }
                                     }
                                  }
                               ]
                            },
                            "actions":[
                               {
                                  "Action":{
                                     "action":"run_playbook",
                                     "action_args":{
                                        "name":"pvc_snapshot_playbook.yml",
                                        "verbosity":3
                                     }
                                  }
                               }
                            ],
                            "enabled":true
                         }
                      }
                   ]
                }\
                """;

        String event = """
                {
                   "type":"MODIFIED",
                   "resource":{
                      "kind":"PersistentVolumeClaim",
                      "apiVersion":"v1",
                      "metadata":{
                         "name":"postgresql",
                         "namespace":"rocketchat",
                         "uid":"98c74400-0165-45c5-a24c-480938bf5b1b",
                         "resourceVersion":"38291427",
                         "creationTimestamp":"2023-03-29T15:56:09Z",
                         "labels":{
                            "template":"postgresql-persistent-template",
                            "template.openshift.io/template-instance-owner":"01a70fda-b7de-4041-9a4b-a004938c88b0"
                         },
                         "annotations":{
                            "volume.kubernetes.io/selected-node":"ip-10-0-133-168.ec2.internal"
                         },
                         "finalizers":[
                            "kubernetes.io/pvc-protection"
                         ],
                         "managedFields":[
                            {
                               "manager":"openshift-controller-manager",
                               "operation":"Update",
                               "apiVersion":"v1",
                               "time":"2023-03-29T15:56:09Z",
                               "fieldsType":"FieldsV1",
                               "fieldsV1":{
                                  "f:metadata":{
                                     "f:labels":{
                                        ".":{
                                          \s
                                        },
                                        "f:template":{
                                          \s
                                        },
                                        "f:template.openshift.io/template-instance-owner":{
                                          \s
                                        }
                                     }
                                  },
                                  "f:spec":{
                                     "f:accessModes":{
                                       \s
                                     },
                                     "f:resources":{
                                        "f:requests":{
                                           ".":{
                                             \s
                                           },
                                           "f:storage":{
                                             \s
                                           }
                                        }
                                     },
                                     "f:volumeMode":{
                                       \s
                                     }
                                  }
                               }
                            },
                            {
                               "manager":"kube-scheduler",
                               "operation":"Update",
                               "apiVersion":"v1",
                               "time":"2023-03-29T15:56:12Z",
                               "fieldsType":"FieldsV1",
                               "fieldsV1":{
                                  "f:metadata":{
                                     "f:annotations":{
                                        ".":{
                                          \s
                                        },
                                        "f:volume.kubernetes.io/selected-node":{
                                          \s
                                        }
                                     }
                                  }
                               }
                            }
                         ]
                      },
                      "spec":{
                         "accessModes":[
                            "ReadWriteOnce"
                         ],
                         "resources":{
                            "requests":{
                               "storage":"1Gi"
                            }
                         },
                         "storageClassName":"gp3-csi",
                         "volumeMode":"Filesystem"
                      },
                      "status":{
                         "phase":"Pending"
                      }
                   },
                   "meta":{
                      "source":{
                         "name":"sabre1041.eda.k8s",
                         "type":"sabre1041.eda.k8s"
                      },
                      "received_at":"2023-03-29T15:56:12.152990Z",
                      "uuid":"ecd616c8-e174-4967-9e26-8968e6197581"
                   }
                }
                """;

        try (AstRulesEngine engine = new AstRulesEngine()) {
            long id = engine.createRuleset(rules);
            String result = engine.assertEvent(id, event);

            assertTrue(result.contains("template.openshift.io/template-instance-owner"));

            List<Map<String, Object>> v = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
            assertEquals(((Map) ((Map) v.get(0).get("Create Snapshot")).get("m")).get("type"), "MODIFIED");
        }
    }
}