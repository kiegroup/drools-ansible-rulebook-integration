package org.drools.ansible.rulebook.integration.test.jpy;

import org.drools.ansible.rulebook.integration.api.JsonTest;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.junit.Assert.*;


public class AstRulesEngineTest {
    @Test
    public void testJpyApi() {
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
            engine.dispose(sessionId);
        }
    }

    @Test
    public void testBrokenApi() throws IOException {
        try (AstRulesEngine engine = new AstRulesEngine();
             InputStream s = getClass().getClassLoader().getResourceAsStream("broken.json")) {
            String rules = new String(s.readAllBytes());
            assertThrows(UnsupportedOperationException.class, () -> engine.createRuleset(rules));
        }
    }

    @Test
    public void testRetractFactFullMatch() throws IOException {
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
            List<Map<String, Object>> factList = readValueAsListOfMapOfStringAndObject(facts);
            assertEquals(1, factList.size());
            assertEquals(factList.get(0), JsonMapper.readValueAsRawObject(fact2));

            String r = engine.retractMatchingFacts(id, retractedFact, false);

            List<Map<String, Object>> v = readValueAsListOfMapOfStringAndObject(r);
            assertEquals(1, v.size());
            assertEquals(((Map) v.get(0).get("r_0")).get("m"), JsonMapper.readValueAsRawObject(fact2));
        }
    }

    @Test
    public void testRetractMatchingFacts() throws IOException {
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

            List<Map<String, Object>> v = readValueAsListOfMapOfStringAndObject(r);

            assertEquals(((Map) v.get(0).get("r_0")).get("m"), JsonMapper.readValueAsRawObject(fact2));
        }
    }

    @Test
    public void testTimedOut() throws IOException {
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
                assertTrue("rule fired after " + elapsed + " milliseconds", elapsed >= 1_900);

                byte[] bytes = bufferedInputStream.readNBytes(l);
                String r = new String(bytes, StandardCharsets.UTF_8);

                List<Object> matches = JsonMapper.readValueExtractFieldAsList(r, "result");
                Map<String, Map> match = (Map<String, Map>) matches.get(0);

                assertNotNull(match.get("r1"));
            }
        }
    }

    @Test
    public void testTimedOutWithAdvance() throws IOException {
        try (AstRulesEngine engine = new AstRulesEngine();
             InputStream s = getClass().getClassLoader().getResourceAsStream("timed_out.json")) {
            int port = engine.port();
            String rules = new String(s.readAllBytes());
            long id = engine.createRuleset(rules);

            try (Socket socket = new Socket("localhost", port)) {
                DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream());

                engine.assertFact(id, "{\"j\": 42}");
                String r = engine.advanceTime(id, 3, "SECONDS");

                List<Map<String, Object>> v = readValueAsListOfMapOfStringAndObject(r);
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

    @Test(timeout = 5000L)
    public void testThrowExceptionOnUnboundSocket() throws IOException {
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
    public void testAssertEvent() throws IOException {
        String rules =
                "{\n" +
                "   \"rules\":[\n" +
                "      {\n" +
                "         \"Rule\":{\n" +
                "            \"name\":\"Create Snapshot\",\n" +
                "            \"condition\":{\n" +
                "               \"AllCondition\":[\n" +
                "                  {\n" +
                "                     \"EqualsExpression\":{\n" +
                "                        \"lhs\":{\n" +
                "                           \"Event\":\"type\"\n" +
                "                        },\n" +
                "                        \"rhs\":{\n" +
                "                           \"String\":\"MODIFIED\"\n" +
                "                        }\n" +
                "                     }\n" +
                "                  }\n" +
                "               ]\n" +
                "            },\n" +
                "            \"actions\":[\n" +
                "               {\n" +
                "                  \"Action\":{\n" +
                "                     \"action\":\"run_playbook\",\n" +
                "                     \"action_args\":{\n" +
                "                        \"name\":\"pvc_snapshot_playbook.yml\",\n" +
                "                        \"verbosity\":3\n" +
                "                     }\n" +
                "                  }\n" +
                "               }\n" +
                "            ],\n" +
                "            \"enabled\":true\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        String event =
                "{\n" +
                "   \"type\":\"MODIFIED\",\n" +
                "   \"resource\":{\n" +
                "      \"kind\":\"PersistentVolumeClaim\",\n" +
                "      \"apiVersion\":\"v1\",\n" +
                "      \"metadata\":{\n" +
                "         \"name\":\"postgresql\",\n" +
                "         \"namespace\":\"rocketchat\",\n" +
                "         \"uid\":\"98c74400-0165-45c5-a24c-480938bf5b1b\",\n" +
                "         \"resourceVersion\":\"38291427\",\n" +
                "         \"creationTimestamp\":\"2023-03-29T15:56:09Z\",\n" +
                "         \"labels\":{\n" +
                "            \"template\":\"postgresql-persistent-template\",\n" +
                "            \"template.openshift.io/template-instance-owner\":\"01a70fda-b7de-4041-9a4b-a004938c88b0\"\n" +
                "         },\n" +
                "         \"annotations\":{\n" +
                "            \"volume.kubernetes.io/selected-node\":\"ip-10-0-133-168.ec2.internal\"\n" +
                "         },\n" +
                "         \"finalizers\":[\n" +
                "            \"kubernetes.io/pvc-protection\"\n" +
                "         ],\n" +
                "         \"managedFields\":[\n" +
                "            {\n" +
                "               \"manager\":\"openshift-controller-manager\",\n" +
                "               \"operation\":\"Update\",\n" +
                "               \"apiVersion\":\"v1\",\n" +
                "               \"time\":\"2023-03-29T15:56:09Z\",\n" +
                "               \"fieldsType\":\"FieldsV1\",\n" +
                "               \"fieldsV1\":{\n" +
                "                  \"f:metadata\":{\n" +
                "                     \"f:labels\":{\n" +
                "                        \".\":{\n" +
                "                           \n" +
                "                        },\n" +
                "                        \"f:template\":{\n" +
                "                           \n" +
                "                        },\n" +
                "                        \"f:template.openshift.io/template-instance-owner\":{\n" +
                "                           \n" +
                "                        }\n" +
                "                     }\n" +
                "                  },\n" +
                "                  \"f:spec\":{\n" +
                "                     \"f:accessModes\":{\n" +
                "                        \n" +
                "                     },\n" +
                "                     \"f:resources\":{\n" +
                "                        \"f:requests\":{\n" +
                "                           \".\":{\n" +
                "                              \n" +
                "                           },\n" +
                "                           \"f:storage\":{\n" +
                "                              \n" +
                "                           }\n" +
                "                        }\n" +
                "                     },\n" +
                "                     \"f:volumeMode\":{\n" +
                "                        \n" +
                "                     }\n" +
                "                  }\n" +
                "               }\n" +
                "            },\n" +
                "            {\n" +
                "               \"manager\":\"kube-scheduler\",\n" +
                "               \"operation\":\"Update\",\n" +
                "               \"apiVersion\":\"v1\",\n" +
                "               \"time\":\"2023-03-29T15:56:12Z\",\n" +
                "               \"fieldsType\":\"FieldsV1\",\n" +
                "               \"fieldsV1\":{\n" +
                "                  \"f:metadata\":{\n" +
                "                     \"f:annotations\":{\n" +
                "                        \".\":{\n" +
                "                           \n" +
                "                        },\n" +
                "                        \"f:volume.kubernetes.io/selected-node\":{\n" +
                "                           \n" +
                "                        }\n" +
                "                     }\n" +
                "                  }\n" +
                "               }\n" +
                "            }\n" +
                "         ]\n" +
                "      },\n" +
                "      \"spec\":{\n" +
                "         \"accessModes\":[\n" +
                "            \"ReadWriteOnce\"\n" +
                "         ],\n" +
                "         \"resources\":{\n" +
                "            \"requests\":{\n" +
                "               \"storage\":\"1Gi\"\n" +
                "            }\n" +
                "         },\n" +
                "         \"storageClassName\":\"gp3-csi\",\n" +
                "         \"volumeMode\":\"Filesystem\"\n" +
                "      },\n" +
                "      \"status\":{\n" +
                "         \"phase\":\"Pending\"\n" +
                "      }\n" +
                "   },\n" +
                "   \"meta\":{\n" +
                "      \"source\":{\n" +
                "         \"name\":\"sabre1041.eda.k8s\",\n" +
                "         \"type\":\"sabre1041.eda.k8s\"\n" +
                "      },\n" +
                "      \"received_at\":\"2023-03-29T15:56:12.152990Z\",\n" +
                "      \"uuid\":\"ecd616c8-e174-4967-9e26-8968e6197581\"\n" +
                "   }\n" +
                "}";

        try (AstRulesEngine engine = new AstRulesEngine()) {
            long id = engine.createRuleset(rules);
            String result = engine.assertEvent(id, event);

            assertTrue(result.contains("template.openshift.io/template-instance-owner"));

            List<Map<String, Object>> v = readValueAsListOfMapOfStringAndObject(result);
            assertEquals(((Map) ((Map) v.get(0).get("Create Snapshot")).get("m")).get("type"), "MODIFIED");
        }
    }
}