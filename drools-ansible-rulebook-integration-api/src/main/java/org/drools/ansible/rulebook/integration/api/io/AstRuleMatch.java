package org.drools.ansible.rulebook.integration.api.io;

import org.drools.core.facttemplates.Fact;
import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.kie.api.runtime.rule.Match;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AstRuleMatch {

    public static List<Map<String, Map>> asList(Collection<Match> matches) {
        return matches.stream()
                .map(AstRuleMatch::from)
                .collect(Collectors.toList());
    }

    public static Map<String, Map> from(Match match) {
        Map<String, Object> facts = new HashMap<>();
        for (String decl : match.getDeclarationIds()) {
            Object value = match.getDeclarationValue(decl);
            if (value instanceof Fact) {
                Fact fact = (Fact) value;
                Map<String, Object> map = RuleMatch.toNestedMap(fact.asMap());
                facts.put(decl, map);
            } else {
                facts.put(decl, value);
            }
        }

        Map<String, Map> result = new HashMap<>();
        result.put(match.getRule().getName(), facts);
        return result;
    }
}
