package org.drools.ansible.rulebook.integration.loadtests;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;

public final class LoadTestMain {

    private static final String DEFAULT_JSON = "24kb_1k_events.json";

    private LoadTestMain() {}

    public static void main(String[] args) {
        String haDbParamsJson = null;
        String haUuid = null;
        boolean failoverRecovery = false;
        List<String> positional = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("--ha-db-params".equals(args[i])) {
                if (i + 1 >= args.length) {
                    System.err.println("ERROR: --ha-db-params requires a JSON argument");
                    System.exit(1);
                }
                haDbParamsJson = args[++i];
            } else if ("--ha-uuid".equals(args[i])) {
                if (i + 1 >= args.length) {
                    System.err.println("ERROR: --ha-uuid requires a value");
                    System.exit(1);
                }
                haUuid = args[++i];
            } else if ("--failover-recovery".equals(args[i])) {
                failoverRecovery = true;
            } else {
                positional.add(args[i]);
            }
        }
        String eventsJson = positional.isEmpty() ? DEFAULT_JSON : positional.get(0);

        // Outcome derived from filename convention:
        //   contains "unmatch"       -> NO_MATCH
        //   starts with "retention_" -> NO_MATCH
        //   otherwise                -> MATCH
        ExpectedOutcome expected = (eventsJson.contains("unmatch") || eventsJson.startsWith("retention_"))
                ? ExpectedOutcome.NO_MATCH
                : ExpectedOutcome.MATCH;

        String rulesJsonRaw = readRulesJson(eventsJson);
        Map jsonObject = rulesJsonRaw.startsWith("[")
                ? (Map) JsonMapper.readValueAsListOfObject(rulesJsonRaw).get(0)
                : JsonMapper.readValueAsMapOfStringAndObject(rulesJsonRaw);
        Map rulesSetMap = (Map) jsonObject.get("RuleSet");
        String rulesetJson = JsonMapper.toJson(rulesSetMap);
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetJson);

        boolean haPg = haDbParamsJson != null;
        Result result;
        if (failoverRecovery) {
            if (haDbParamsJson == null) {
                throw new IllegalArgumentException("--ha-db-params is required for --failover-recovery mode");
            }
            if (haUuid == null) {
                throw new IllegalArgumentException("--ha-uuid is required for --failover-recovery mode");
            }
            result = HaFailoverRecoveryRunner.runRecovery(rulesSet, rulesetJson, haDbParamsJson, haUuid);
        } else if (haPg) {
            result = HaLoadRunner.runLoad(rulesSet, rulesetJson, rulesSetMap, haDbParamsJson, expected, eventsJson, haUuid);
        } else {
            result = LoadRunner.run(rulesSet, rulesSetMap, expected, eventsJson);
        }

        MetricReporter.report(System.err, eventsJson, haPg, failoverRecovery, result.usedMemoryBytes, result.durationMs);
    }

    private static String readRulesJson(String name) {
        try (InputStream is = LoadTestMain.class.getClassLoader().getResourceAsStream(name)) {
            if (is != null) {
                return new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (InputStream is = new FileInputStream(name)) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Rules JSON not found on classpath or filesystem: " + name, e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
