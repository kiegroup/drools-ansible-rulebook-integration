package org.drools.ansible.rulebook.integration.api.domain;

import org.kie.api.runtime.rule.Match;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.factToMap;

public class RuleMatch {

    private final String ruleName;
    private final Map<String, Object> facts;

    public RuleMatch(String ruleName, Map<String, Object> facts) {
        this.ruleName = ruleName;
        this.facts = facts;
    }

    public static RuleMatch from(Match match) {
        return new RuleMatch(match.getRule().getName(), matchToMap(match));
    }

    public static List<Map<String, Map>> asList(Collection<Match> matches) {
        return matches.stream()
                .map(RuleMatch::asMap)
                .collect(Collectors.toList());
    }

    private static Map<String, Map> asMap(Match match) {
        Map<String, Map> result = new HashMap<>();
        result.put(match.getRule().getName(), matchToMap(match));
        return result;
    }

    private static Map<String, Object> matchToMap(Match match) {
        Map<String, Object> facts = new HashMap<>();
        for (String decl : match.getDeclarationIds()) {
            facts.put(decl, factToMap( match.getDeclarationValue(decl) ) );
        }
        return facts;
    }

    public String getRuleName() {
        return ruleName;
    }

    public Object getFact(String id) {
        return facts.get(id);
    }

    public int getFactsSize() {
        return facts.size();
    }

    @Override
    public String toString() {
        return "rule \"" + ruleName + "\" with facts: " + facts;
    }

    public Map<Object, Object> getFacts() {
        return null;
    }
}
