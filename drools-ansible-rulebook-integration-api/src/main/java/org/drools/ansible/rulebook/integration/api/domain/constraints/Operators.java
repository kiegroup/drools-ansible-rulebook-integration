package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.isRegexOperator;
import static org.drools.ansible.rulebook.integration.api.domain.constraints.ListContainsConstraint.listContains;

public class Operators {

    private static final Map<String, BiPredicate> OPERATORS_MAP = new HashMap<>();

    static {
        OPERATORS_MAP.put("==", (a,b) -> a.equals(b));
        OPERATORS_MAP.put("!=", (a,b) -> !a.equals(b));
        OPERATORS_MAP.put(">", (a,b) -> ((Comparable) a).compareTo(b) > 0);
        OPERATORS_MAP.put(">=", (a,b) -> ((Comparable) a).compareTo(b) >= 0);
        OPERATORS_MAP.put("<", (a,b) -> ((Comparable) a).compareTo(b) < 0);
        OPERATORS_MAP.put("<=", (a,b) -> ((Comparable) a).compareTo(b) <= 0);
        OPERATORS_MAP.put("in", (a,b) -> listContains(b, a));
        OPERATORS_MAP.put("not in", (a,b) -> !listContains(b, a));
        OPERATORS_MAP.put("contains", (a,b) -> listContains(a, b));
        OPERATORS_MAP.put("not contains", (a,b) -> !listContains(a, b));
    }

    public static Predicate<?> toOperatorPredicate(String operator, Object value) {
        String regexPattern = value instanceof String && isRegexOperator(operator) ? (String)value : null;
        if (regexPattern != null) {
            Pattern pattern = Pattern.compile(regexPattern);
            return a -> a != null && pattern.matcher(a.toString()).find();
        }


        BiPredicate op = OPERATORS_MAP.get(operator);
        if (op == null) {
            throw new UnsupportedOperationException("Unknown operator: " + operator);
        }
        return a -> a != null ? op.test(a, value) : value == null;
    }
}
