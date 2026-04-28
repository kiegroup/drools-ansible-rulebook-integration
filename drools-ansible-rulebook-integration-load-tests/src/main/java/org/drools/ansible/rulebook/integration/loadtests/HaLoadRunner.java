package org.drools.ansible.rulebook.integration.loadtests;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HaLoadRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(HaLoadRunner.class);

    private HaLoadRunner() {}

    public static Result runLoad(RulesSet rulesSet, String rulesetJson, Map rulesSetMap,
                                 String haDbParamsJson, ExpectedOutcome expected, String eventsJson,
                                 String haUuidOverride) {
        try (AstRulesEngine engine = new AstRulesEngine()) {
            String haUuid = haUuidOverride != null
                    ? haUuidOverride
                    : "loadtest-ha-" + System.currentTimeMillis();
            engine.initializeHA(haUuid, "loadtest-worker", haDbParamsJson, "{\"write_after\":1}");

            long id = engine.createRuleset(rulesSet, rulesetJson);
            int port = engine.port();

            Socket haSocket;
            try {
                haSocket = new Socket("localhost", port);
            } catch (IOException e) {
                throw new RuntimeException("Failed to connect HA socket", e);
            }

            try {
                engine.enableLeader();

                LOGGER.info("*** Start measuring execution time");
                Measurement.TimedResult t = Measurement.timeWork(() -> {
                    Payload payload = Payload.parsePayload(rulesSetMap);
                    return payload.execute(engine, id);
                });
                LOGGER.info("*** End measuring execution time, duration = {} ms", t.durationMs);

                OutcomeCheck.verify(t.matchCount, expected, eventsJson);

                String stats = engine.sessionStats(id);
                LOGGER.info(stats);

                long mem = Measurement.captureUsedMemoryAfterGc();

                return new Result(t.matches, t.durationMs, mem);
            } finally {
                try {
                    haSocket.close();
                } catch (IOException e) {
                    LOGGER.warn("Failed to close HA socket", e);
                }
            }
        }
    }
}
