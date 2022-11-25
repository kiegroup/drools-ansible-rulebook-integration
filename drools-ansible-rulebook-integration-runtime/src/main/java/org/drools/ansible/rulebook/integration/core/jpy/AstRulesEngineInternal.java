package org.drools.ansible.rulebook.integration.core.jpy;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.kie.api.runtime.rule.Match;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

class AstRulesEngineInternal {

    public long createRuleset(RulesSet rulesSet) {
        RulesExecutor executor = RulesExecutorFactory.createRulesExecutor(rulesSet);
        return executor.getId();
    }

    public void dispose(long sessionId) {
        RulesExecutor rulesExecutor = RulesExecutorContainer.INSTANCE.get(sessionId);
        if (rulesExecutor == null) return; // ignore, already disposed
        rulesExecutor.dispose();
    }

    /**
     * @return error code (currently always 0)
     */
    public List<Map<String, ?>> retractFact(long sessionId, Map<String, Object> fact) {
        Map<String, Object> boundFact = Map.of("m", fact);
        List<Map<String, Map>> objs = processMessage(
                fact,
                RulesExecutorContainer.INSTANCE.get(sessionId)::processRetract);
        List<Map<String, ?>> results = objs.stream()
                .map(m -> m.entrySet().stream().findFirst()
                        .map(e -> Map.of(e.getKey(), boundFact)).get())
                .collect(Collectors.toList());
        return results;
    }

    public List<Map<String, Map>> assertFact(long sessionId, Map<String, Object> fact) {
        return processMessage(
                fact,
                RulesExecutorContainer.INSTANCE.get(sessionId)::processFacts);
    }

    public List<Map<String, Map>> assertEvent(long sessionId, Map<String, Object> fact) {
        return processMessage(
                fact,
                RulesExecutorContainer.INSTANCE.get(sessionId)::processEvents);
    }

    public List<Map<String, Object>> getFacts(long sessionId) {
        RulesExecutor executor = RulesExecutorContainer.INSTANCE.get(sessionId);
        if (executor == null) {
            throw new NoSuchElementException("No such session id: " + sessionId + ". " +
                    "Was it disposed?");
        }
        return executor.getAllFactsAsMap();
    }

    /**
     * Advances the clock time in the specified unit amount.
     *
     * @param amount the amount of units to advance in the clock
     * @param unit   the used time unit
     * @return the events that fired
     */
    public List<Match> advanceTime(long sessionId, long amount, String unit) {
        return RulesExecutorContainer.INSTANCE.get(sessionId)
                .advanceTime(amount, TimeUnit.valueOf(unit.toUpperCase()));
    }


    private List<Map<String, Map>> processMessage(Map<String, Object> fact, Function<Map<String, Object>, Collection<Match>> command) {
        return AstRuleMatch.asList(command.apply(fact));
    }

}
