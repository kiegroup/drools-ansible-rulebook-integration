package org.drools.ansible.rulebook.integration.api.rulesengine;

import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.drools.core.common.InternalFactHandle;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RegisterOnlyAgendaFilter implements AgendaFilter {

    protected static final Logger log = LoggerFactory.getLogger(RegisterOnlyAgendaFilter.class);

    public static final String SYNTHETIC_RULE_TAG = "SYNTHETIC_RULE";
    public static final String RULE_TYPE_TAG = "RULE_TYPE";
    public static final Map<String, Function<Match, Match>> matchTransformers = new HashMap<>();

    private final RulesExecutorSession rulesExecutorSession;

    private final Set<Match> matchedRules = new LinkedHashSet<>();

    private final Set<FactHandle> eventsToBeDeleted = Collections.newSetFromMap(new IdentityHashMap<>());

    public RegisterOnlyAgendaFilter(RulesExecutorSession rulesExecutorSession) {
        this.rulesExecutorSession = rulesExecutorSession;
    }

    @Override
    public boolean accept(Match match) {
        List<InternalFactHandle> fhs = (List<InternalFactHandle>) match.getFactHandles();
        boolean validMatch = isValidMatch(fhs);

        if (validMatch) {
            if (log.isInfoEnabled()) {
                log.info(matchToString(match));
            }

            Map<String, Object> metadata = match.getRule().getMetaData();
            if ( metadata.get(SYNTHETIC_RULE_TAG) != null ) {
                return true;
            }

            matchedRules.add( matchTransformers.getOrDefault(metadata.get(RULE_TYPE_TAG), Function.identity()).apply(match) );
        }

        for (InternalFactHandle fh : fhs) {
            if (fh.isEvent()) {
                eventsToBeDeleted.add(fh);
            }
        }

        return validMatch;
    }

    public List<Match> finalizeAndGetResults(boolean event) {
        rulesExecutorSession.registerMatchedEvents(eventsToBeDeleted);
        for (FactHandle toBeDeleted : eventsToBeDeleted) {
            rulesExecutorSession.delete(toBeDeleted);
        }
        eventsToBeDeleted.clear();

        List<Match> matches = new ArrayList<>( matchedRules );
        matchedRules.clear();

        if (event && matches.size() > 1) {
            String firstMatchedRuleName = matches.get(0).getRule().getName();
            matches = matches.stream().takeWhile(match -> match.getRule().getName().equals(firstMatchedRuleName)).collect(Collectors.toList());
        }

        matches.forEach(rulesExecutorSession::registerMatch);
        return matches;
    }

    public static void registerMatchTransformer(String ruleType, Function<Match, Match> transformer) {
        matchTransformers.put(ruleType, transformer);
    }

    private static String matchToString(Match match) {
        Map<String, Object> metadata = match.getRule().getMetaData();
        String ruleType = metadata.get(SYNTHETIC_RULE_TAG) != null ? " synthetic " : " effective ";
        return "Activation of" + ruleType + RuleMatch.from(match);
    }

    private static boolean isValidMatch(List<InternalFactHandle> fhs) {
        return !isSelfJoin(fhs);
    }

    private static boolean isSelfJoin(List<InternalFactHandle> fhs) {
        for (int i = 0; i < fhs.size()-1; i++) {
            for (int j = i+1; j < fhs.size(); j++) {
                if (fhs.get(i) == fhs.get(j)) {
                    return true;
                }
            }
        }
        return false;
    }
}