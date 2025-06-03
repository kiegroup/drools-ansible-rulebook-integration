package org.drools.ansible.rulebook.integration.test.jpy;

import org.drools.ansible.rulebook.integration.api.JsonTest;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AsyncAstRulesEngine;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class AsyncAstRulesEngineTest {
    @Test
    void testJpyApi() throws Exception {

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

        } finally {
            engine.shutdown();
        }
    }

    @Test
    void testBrokenApi() throws IOException {
        AsyncAstRulesEngine engine = new AsyncAstRulesEngine();
        try (InputStream s = getClass().getClassLoader().getResourceAsStream("broken.json")) {
            String rules = new String(s.readAllBytes());
            assertThrows(UnsupportedOperationException.class, () -> engine.createRuleset(rules));
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void testRetractFact() throws IOException {
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
            }

            {
                engine.assertFact(id, "{\"i\": 67}");
                // this produces no rule firing, so no response is produced
                // int l = bufferedInputStream.readInt();
                // byte[] bytes = bufferedInputStream.readNBytes(l);
            }

            String retractedFact = "{\"i\": 67}";
            {
                engine.retractFact(id, retractedFact);
                int l = bufferedInputStream.readInt();
                byte[] bytes = bufferedInputStream.readNBytes(l);
                String r = new String(bytes, StandardCharsets.UTF_8);
                
                assertEquals(JsonMapper.readValueAtAsRawObject(r, "/result/0/r_0/m"),
                        Map.of("i", 67));
            }
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void testGetFacts() throws IOException {

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
            }

            {
                engine.assertFact(id, "{\"i\": 67}");
                // this produces no rule firing, so no response is produced
                // int l = bufferedInputStream.readInt();
                // byte[] bytes = bufferedInputStream.readNBytes(l);
            }

            String result = engine.getFacts(id);
            assertNotNull(result);
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void testShutdown() throws Exception{
        AsyncAstRulesEngine engine = new AsyncAstRulesEngine();

        try (InputStream s = getClass().getClassLoader().getResourceAsStream("retract_fact.json")) {
            String rules = new String(s.readAllBytes());
            engine.createRuleset(rules);
        }
        
        engine.shutdown();
    }

}