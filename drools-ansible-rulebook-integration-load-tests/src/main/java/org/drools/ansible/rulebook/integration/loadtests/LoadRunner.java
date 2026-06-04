package org.drools.ansible.rulebook.integration.loadtests;

import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LoadRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadRunner.class);

    private LoadRunner() {}

    public static Result run(RulesSet rulesSet, Map rulesSetMap,
                             ExpectedOutcome expected, String eventsJson) {
        try (AstRulesEngine engine = new AstRulesEngine()) {
            long id = engine.createRuleset(rulesSet);

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
        }
    }
}
