package org.drools.ansible.rulebook.integration.loadtests;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the Phase 2 (Recovery) half of a two-JVM failover load test.
 *
 * Cold-starts a new AstRulesEngine against the same PG state written by a
 * prior {@link HaLoadRunner} invocation with the same {@code haUuid}, times
 * {@code engine.enableLeader()} (which triggers session recovery from PG),
 * and returns the recovery duration + post-GC memory snapshot.
 *
 * No payload execution. No OutcomeCheck call.
 */
public final class HaFailoverRecoveryRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(HaFailoverRecoveryRunner.class);

    private HaFailoverRecoveryRunner() {}

    public static Result runRecovery(RulesSet rulesSet, String rulesetJson,
                                     String haDbParamsJson, String haUuid) {
        try (AstRulesEngine engine = new AstRulesEngine()) {
            engine.initializeHA(haUuid, "recovery-worker", haDbParamsJson, "{\"write_after\":1}");

            long id = engine.createRuleset(rulesSet, rulesetJson);
            int port = engine.port();

            Socket haSocket;
            try {
                haSocket = new Socket("localhost", port);
            } catch (IOException e) {
                throw new RuntimeException("Failed to connect HA socket", e);
            }

            try {
                LOGGER.info("*** Start measuring recovery time (enableLeader)");
                Measurement.TimedResult t = Measurement.timeWork(() -> {
                    engine.enableLeader();
                    return Payload.Execution.empty();
                });
                LOGGER.info("*** End measuring recovery time, duration = {} ms", t.durationMs);

                String stats = engine.sessionStats(id);
                LOGGER.info(stats);

                long mem = Measurement.captureUsedMemoryAfterGc();

                return new Result(List.of(), t.durationMs, mem);
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
