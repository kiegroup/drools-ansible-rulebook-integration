package org.drools.ansible.rulebook.integration.migrationtests;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;

public final class MigrationTestMain {

    private static final String DEFAULT_RULESET = "migration_partial_match.json";
    private static final int PERSIST_EVENT_COUNT = 10;

    private MigrationTestMain() {}

    public static void main(String[] args) {
        String mode = null;
        String haDbParams = null;
        String haUuid = null;
        String rulesetFile = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--persist":
                    mode = "persist";
                    break;
                case "--verify":
                    mode = "verify";
                    break;
                case "--ha-db-params":
                    if (i + 1 >= args.length) {
                        System.err.println("ERROR: --ha-db-params requires a JSON argument");
                        System.exit(1);
                    }
                    haDbParams = args[++i];
                    break;
                case "--ha-uuid":
                    if (i + 1 >= args.length) {
                        System.err.println("ERROR: --ha-uuid requires a value");
                        System.exit(1);
                    }
                    haUuid = args[++i];
                    break;
                default:
                    rulesetFile = args[i];
                    break;
            }
        }

        if (mode == null) {
            System.err.println("ERROR: --persist or --verify is required");
            System.exit(1);
        }
        if (haDbParams == null) {
            System.err.println("ERROR: --ha-db-params is required");
            System.exit(1);
        }
        if (haUuid == null) {
            System.err.println("ERROR: --ha-uuid is required");
            System.exit(1);
        }

        String rulesetJson = extractRulesetJson(rulesetFile != null ? rulesetFile : DEFAULT_RULESET);

        try {
            if ("persist".equals(mode)) {
                runPersist(rulesetJson, haDbParams, haUuid);
            } else {
                boolean matched = runVerify(rulesetJson, haDbParams, haUuid);
                if (!matched) {
                    System.err.println("VERIFY FAILED: no matches found — partial matches were not recovered");
                    System.exit(1);
                }
                System.err.println("VERIFY OK: partial matches recovered successfully");
            }
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void runPersist(String rulesetJson, String haDbParams, String haUuid) {
        try (AstRulesEngine engine = new AstRulesEngine()) {
            engine.initializeHA(haUuid, "persist-worker", haDbParams, "{\"write_after\":1}");
            long id = engine.createRuleset(rulesetJson);

            try (Socket socket = new Socket("localhost", engine.port())) {
                engine.enableLeader();

                for (int i = 0; i < PERSIST_EVENT_COUNT; i++) {
                    String event = "{\"meta\":{\"uuid\":\"" + UUID.randomUUID() + "\"},\"a\":1}";
                    engine.assertEvent(id, event);
                }

                System.out.println(engine.sessionStats(id));
            }
        } catch (IOException e) {
            throw new RuntimeException("Socket error", e);
        }
    }

    private static boolean runVerify(String rulesetJson, String haDbParams, String haUuid) {
        try (AstRulesEngine engine = new AstRulesEngine()) {
            engine.initializeHA(haUuid, "verify-worker", haDbParams, "{\"write_after\":1}");
            long id = engine.createRuleset(rulesetJson);

            try (Socket socket = new Socket("localhost", engine.port())) {
                engine.enableLeader();

                String event = "{\"meta\":{\"uuid\":\"" + UUID.randomUUID() + "\"},\"b\":1}";
                String result = engine.assertEvent(id, event);
                List<Map<String, Object>> matches = JsonMapper.readValueAsListOfMapOfStringAndObject(result);

                System.out.println(engine.sessionStats(id));
                return !matches.isEmpty();
            }
        } catch (IOException e) {
            throw new RuntimeException("Socket error", e);
        }
    }

    private static String extractRulesetJson(String name) {
        String raw = readRulesJson(name);
        Map jsonObject = raw.startsWith("[")
                ? (Map) JsonMapper.readValueAsListOfObject(raw).get(0)
                : JsonMapper.readValueAsMapOfStringAndObject(raw);
        Map rulesSetMap = (Map) jsonObject.get("RuleSet");
        return JsonMapper.toJson(rulesSetMap);
    }

    private static String readRulesJson(String name) {
        try (InputStream is = MigrationTestMain.class.getClassLoader().getResourceAsStream(name)) {
            if (is != null) {
                return new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (InputStream is = new FileInputStream(name)) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Rules JSON not found on classpath or filesystem: " + name, e);
        }
    }
}
