package org.drools.ansible.rulebook.integration.test.jpy;

import org.drools.ansible.rulebook.integration.api.JsonTest;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.json.JSONObject;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class AstRulesEngineTest {
    @Test
    public void testJpyApi() {

        String rules = JsonTest.JSON1;

        AstRulesEngine engine = new AstRulesEngine();
        long id = engine.createRuleset(rules);

        String result = engine.assertFact(id, "{ \"sensu\": { \"data\": { \"i\":1 } } }");

        assertNotNull(result);
    }

    @Test
    public void testBrokenApi() throws IOException {
        try (InputStream s = getClass().getClassLoader().getResourceAsStream("broken.json")) {
            String rules = new String(s.readAllBytes());

            AstRulesEngine engine = new AstRulesEngine();
            assertThrows(UnsupportedOperationException.class, () -> engine.createRuleset(rules));
        }
    }

    @Test
    public void testRetractFact() throws IOException {
        try (InputStream s = getClass().getClassLoader().getResourceAsStream("retract_fact.json")) {
            String rules = new String(s.readAllBytes());

            AstRulesEngine engine = new AstRulesEngine();
            long id = engine.createRuleset(rules);
            engine.assertFact(id, "{\"j\": 42}");
            engine.assertFact(id, "{\"i\": 67}");
            String retractedFact = "{\"i\": 67}";
            String r = engine.retractFact(id, retractedFact);

            List<Map<String, Map>> v = readValue(r);

            assertEquals(v.get(0).get("r_0").get("m"), new JSONObject(retractedFact).toMap());
        }
    }

    @Test
    public void testTimedOut() throws IOException {
        try (InputStream s = getClass().getClassLoader().getResourceAsStream("timed_out.json")) {
            AstRulesEngine engine = new AstRulesEngine();
            String rules = new String(s.readAllBytes());
            long id = engine.createRuleset(rules);

            int port = engine.port();

            try (Socket socket = new Socket("localhost", port)) {
                DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream());

                long assertTime = System.currentTimeMillis();
                engine.assertFact(id, "{\"j\": 42}");

                int l = bufferedInputStream.readInt();
                long firingTime = System.currentTimeMillis();

                // fires after at least 2 seconds
                assertTrue((firingTime - assertTime) >= 2000);

                byte[] bytes = bufferedInputStream.readNBytes(l);
                String r = new String(bytes, StandardCharsets.UTF_8);
                JSONObject v = new JSONObject(r);

                List<Object> matches = v.getJSONArray("result").toList();
                Map<String, Map> match = (Map<String, Map>) matches.get(0);

                assertNotNull(match.get("r1"));

                engine.shutdown();
            }
        }
    }

    @Test
    public void testTimedOutWithAdvance() throws IOException {
        try (InputStream s = getClass().getClassLoader().getResourceAsStream("timed_out.json")) {
            AstRulesEngine engine = new AstRulesEngine();
            String rules = new String(s.readAllBytes());
            long id = engine.createRuleset(rules);

            int port = engine.port();

            try (Socket socket = new Socket("localhost", port)) {
                DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream());

                engine.assertFact(id, "{\"j\": 42}");
                String r = engine.advanceTime(id, 3, "SECONDS");

                List<Map<String, Map>> v = readValue(r);
                assertNotNull(v.get(0).get("r1"));

                int l = bufferedInputStream.readInt();
                byte[] bytes = bufferedInputStream.readNBytes(l);
                String r2 = new String(bytes, StandardCharsets.UTF_8);

                List<Object> matches2 = new JSONObject(r2).getJSONArray("result").toList();
                Map<String, Map> match = (Map<String, Map>) matches2.get(0);
                assertNotNull(match.get("r1"));

                engine.shutdown();
            }
        }
    }
}