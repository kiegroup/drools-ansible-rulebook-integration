package org.drools.ansible.rulebook.integration.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.drools.ansible.rulebook.integration.api.io.AstRuleMatch;
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.core.common.InternalFactHandle;
import org.drools.core.facttemplates.Fact;
import org.json.JSONObject;
import org.kie.api.definition.rule.Rule;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.mapToFact;

public class RulesExecutor {

    public static final String SYNTHETIC_RULE_TAG = "SYNTHETIC_RULE";

    private final RulesExecutorSession rulesExecutorSession;

    private final long id;

    private final Set<Long> ephemeralFactHandleIds = ConcurrentHashMap.newKeySet();

    private final boolean async;

    private RulesExecutorContainer rulesExecutorContainer;

    RulesExecutor(RulesExecutorSession rulesExecutorSession, boolean async, long id) {
        this.rulesExecutorSession = rulesExecutorSession;
        this.async = async;
        this.id = id;
    }

    public void setRulesExecutorContainer(RulesExecutorContainer rulesExecutorContainer) {
        this.rulesExecutorContainer = rulesExecutorContainer;
    }

    public long getId() {
        return id;
    }

    public void dispose() {
        if (rulesExecutorContainer != null) {
            rulesExecutorContainer.dispose(this);
        }
        rulesExecutorSession.dispose();
    }

    public long rulesCount() {
        return rulesExecutorSession.rulesCount();
    }

    public int executeFacts(String json) {
        return executeFacts( new JSONObject(json).toMap() );
    }

    public int executeFacts(Map<String, Object> factMap) {
        insertFact( factMap, false );
        return rulesExecutorSession.fireAllRules();
    }

    public List<Match> processFacts(String json) {
        return processFacts( new JSONObject(json).toMap() );
    }

    public List<Match> processFacts(Map<String, Object> factMap) {
        return process(factMap, false);
    }

    public List<Match> processEvents(String json) {
        return processEvents( new JSONObject(json).toMap() );
    }

    public List<Match> processEvents(Map<String, Object> factMap) {
        return process(factMap, true);
    }

    public List<Match> fire() {
        return getMatches(false);
    }

    private List<Match> process(Map<String, Object> factMap, boolean event) {
        Collection<FactHandle> fhs = insertFacts(factMap, event);
        if (event) {
            fhs.stream()
                    .map(InternalFactHandle.class::cast)
                    .map(InternalFactHandle::getId)
                    .forEach(ephemeralFactHandleIds::add);
        }
        return writeResponse( getMatches(event) );
    }

    private List<Match> writeResponse(List<Match> matches) {
        // TODO: in case of async execution also make async the rule evaluation
        if (async && !matches.isEmpty()) { // skip empty result
            rulesExecutorContainer.write(new Response(id, AstRuleMatch.asList(matches)));
        }
        return matches;
    }

    private List<Match> getMatches(boolean event) {
        List<Match> matches = findMatchedRules();
        return !event || matches.size() < 2 ?
                matches :
                // when processing an event return only the matches for the first matched rule
                matches.stream().takeWhile(match -> match.getRule().getName().equals(matches.get(0).getRule().getName())).collect(Collectors.toList());
    }

    private List<Match> findMatchedRules() {
        rulesExecutorSession.setExecuteActions(false);
        RegisterOnlyAgendaFilter filter = new RegisterOnlyAgendaFilter(rulesExecutorSession, ephemeralFactHandleIds);
        rulesExecutorSession.fireAllRules(filter);
        rulesExecutorSession.setExecuteActions(true);
        return filter.finalizeAndGetResults();
    }

    public boolean executeActions() {
        return rulesExecutorSession.isExecuteActions();
    }

    private Collection<FactHandle> insertFacts(Map<String, Object> factMap, boolean event) {
        String key = event ? "events" : "facts";
        if (factMap.size() == 1 && factMap.containsKey(key)) {
            return ((List<Map<String, Object>>)factMap.get(key)).stream()
                    .flatMap(map -> this.insertFacts(map, event).stream())
                    .collect(Collectors.toList());
        } else {
            return Collections.singletonList( insertFact(factMap, event) );
        }
    }

    private FactHandle insertFact(Map<String, Object> factMap, boolean event) {
        return rulesExecutorSession.insert( mapToFact(factMap, event) );
    }

    public int executeRetract(String json) {
        return retractFact( new JSONObject(json).toMap() ) ? rulesExecutorSession.fireAllRules() : 0;
    }

    public List<Match> processRetract(String json) {
        return processRetract(new JSONObject(json).toMap());
    }

    public List<Match> processRetract(Map<String, Object> json) {
        List<Match> matches = retractFact(json) ? findMatchedRules() : Collections.emptyList();
        matches = matches.stream().map( MatchDecorator::new ).map( m -> m.withBoundObject("m", json) ).collect(Collectors.toList());
        return writeResponse( matches );
    }

    public boolean retractFact(Map<String, Object> factMap) {
        return rulesExecutorSession.deleteFact( mapToFact(factMap, false) );
    }

    public Collection<?> getAllFacts() {
        return rulesExecutorSession.getObjects();
    }

    public List<Map<String, Object>> getAllFactsAsMap() {
        return getAllFacts().stream().map(Fact.class::cast).map(Fact::asMap).collect(Collectors.toList());
    }

    public String getAllFactsAsJson() {
        return toJson(getAllFactsAsMap());
    }

    public List<Match> advanceTime(long amount, TimeUnit unit ) {
        rulesExecutorSession.advanceTime(amount, unit);
        return writeResponse( findMatchedRules() );
    }

    private static class RegisterOnlyAgendaFilter implements AgendaFilter {

        private final RulesExecutorSession rulesExecutorSession;
        private final Set<Long> ephemeralFactHandleIds;

        private final Set<Match> matchedRules = new LinkedHashSet<>();

        private final List<FactHandle> factsToBeDeleted = new ArrayList<>();

        private RegisterOnlyAgendaFilter(RulesExecutorSession rulesExecutorSession, Set<Long> ephemeralFactHandleIds) {
            this.rulesExecutorSession = rulesExecutorSession;
            this.ephemeralFactHandleIds = ephemeralFactHandleIds;
        }

        @Override
        public boolean accept(Match match) {
            if ( match.getRule().getMetaData().get(SYNTHETIC_RULE_TAG) != null ) {
                return true;
            }
            matchedRules.add(match);
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
            return new ArrayList<>( matchedRules );
        }
    }

    static class MatchDecorator implements Match {
        private final Match delegate;
        private final Map<String, Object> boundObjects = new HashMap<>();

        MatchDecorator(Match delegate) {
            this.delegate = delegate;
        }

        @Override
        public Rule getRule() {
            return delegate.getRule();
        }

        @Override
        public List<? extends FactHandle> getFactHandles() {
            return delegate.getFactHandles();
        }

        @Override
        public List<Object> getObjects() {
            List<Object> objects = new ArrayList<>();
            objects.addAll( delegate.getObjects() );
            objects.addAll( boundObjects.values() );
            return objects;
        }

        @Override
        public List<String> getDeclarationIds() {
            List<String> ids = new ArrayList<>();
            ids.addAll( delegate.getDeclarationIds() );
            ids.addAll( boundObjects.keySet() );
            return ids;
        }

        @Override
        public Object getDeclarationValue(String declarationId) {
            Object object = boundObjects.get(declarationId);
            if (object == null) {
                object = delegate.getDeclarationValue(declarationId);
            }
            return object;
        }

        public MatchDecorator withBoundObject(String bindingName, Object fact) {
            boundObjects.put(bindingName, fact);
            return this;
        }
    }
}
