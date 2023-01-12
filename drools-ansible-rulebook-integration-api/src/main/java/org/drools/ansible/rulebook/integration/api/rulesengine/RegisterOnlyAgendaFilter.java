package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.drools.core.common.InternalFactHandle;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;

public class RegisterOnlyAgendaFilter implements AgendaFilter {

    public static final String SYNTHETIC_RULE_TAG = "SYNTHETIC_RULE";
    public static final String RULE_TYPE_TAG = "RULE_TYPE";
    public static final Map<String, Function<Match, Match>> matchTransformers = new HashMap<>();

    private final RulesExecutorSession rulesExecutorSession;

    private final Set<Long> ephemeralFactHandleIds = ConcurrentHashMap.newKeySet();

    private final Set<Match> matchedRules = new LinkedHashSet<>();

    private final List<FactHandle> factsToBeDeleted = new ArrayList<>();

    public RegisterOnlyAgendaFilter(RulesExecutorSession rulesExecutorSession) {
        this.rulesExecutorSession = rulesExecutorSession;
    }

    public void registerEphemeralFact(Long factId) {
        ephemeralFactHandleIds.add(factId);
    }

    @Override
    public boolean accept(Match match) {
        Map<String, Object> metadata = match.getRule().getMetaData();
        if ( metadata.get(SYNTHETIC_RULE_TAG) != null ) {
            return true;
        }

        matchedRules.add( matchTransformers.getOrDefault(metadata.get(RULE_TYPE_TAG), Function.identity()).apply(match) );

        if (!ephemeralFactHandleIds.isEmpty()) {
            for (FactHandle fh : match.getFactHandles()) {
                if (ephemeralFactHandleIds.remove(((InternalFactHandle) fh).getId())) {
                    factsToBeDeleted.add(fh);
                }
            }
        }
        return true;
    }

    public List<Match> finalizeAndGetResults() {
        factsToBeDeleted.forEach(rulesExecutorSession::delete);
        List<Match> matches = new ArrayList<>( matchedRules );
        matchedRules.clear();
        return matches;
    }

    public static void registerMatchTransformer(String ruleType, Function<Match, Match> transformer) {
        matchTransformers.put(ruleType, transformer);
    }
}