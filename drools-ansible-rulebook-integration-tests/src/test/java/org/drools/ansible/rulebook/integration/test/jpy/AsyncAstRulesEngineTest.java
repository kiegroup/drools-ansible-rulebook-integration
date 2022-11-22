package org.drools.ansible.rulebook.integration.test.jpy;

import com.fasterxml.jackson.core.type.TypeReference;
import org.drools.ansible.rulebook.integration.api.JsonTest;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.core.jpy.AsyncAstRulesEngine;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;


public class AsyncAstRulesEngineTest {
    @Test
    public void testJpyApi() throws Exception {

        String rules = JsonTest.JSON1;

        AsyncAstRulesEngine engine = new AsyncAstRulesEngine();
        int port = engine.port();

        try (Socket socket = new Socket("localhost", port)) {
            DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream());
            long id = engine.createRuleset(rules);


            engine.assertFact(id, "{ \"sensu\": { \"data\": { \"i\":1 } } }");

            int l = bufferedInputStream.readInt();
            byte[] bytes = bufferedInputStream.readNBytes(l);
            String result = new String(bytes, StandardCharsets.UTF_8);

            assertNotNull(result);

        }

    }

    @Test
    @Ignore("Not yet implemented")
    public void testBrokenApi() throws IOException {
        try (InputStream s = getClass().getClassLoader().getResourceAsStream("broken.json")) {
            String rules = new String(s.readAllBytes());

            AsyncAstRulesEngine engine = new AsyncAstRulesEngine();
            assertThrows(UnsupportedOperationException.class, () -> engine.createRuleset(rules));
        }
    }

    @Test
    public void testRetractFact() throws IOException {
        AsyncAstRulesEngine engine = new AsyncAstRulesEngine();
        int port = engine.port();
        try (InputStream s = getClass().getClassLoader().getResourceAsStream("retract_fact.json");
             Socket socket = new Socket("localhost", port)) {

            DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream());
            String rules = new String(s.readAllBytes());

            long id = engine.createRuleset(rules);

            {
                engine.assertFact(id, "{\"j\": 42}");
                int l = bufferedInputStream.readInt();
                byte[] bytes = bufferedInputStream.readNBytes(l);
                String result = new String(bytes, StandardCharsets.UTF_8);
            }

            {
                engine.assertFact(id, "{\"i\": 67}");
                int l = bufferedInputStream.readInt();
                byte[] bytes = bufferedInputStream.readNBytes(l);
                String result = new String(bytes, StandardCharsets.UTF_8);
            }

            String retractedFact = "{\"i\": 67}";
            {
                engine.retractFact(id, retractedFact);
                int l = bufferedInputStream.readInt();
                byte[] bytes = bufferedInputStream.readNBytes(l);
                String r = new String(bytes, StandardCharsets.UTF_8);
                JSONObject v = new JSONObject(r);
                assertEquals(v.getJSONArray("result").getJSONObject(0).getJSONObject("r_0").get("m").toString(),
                        new JSONObject().put("i", 67).toString());
            }

        }
    }

}