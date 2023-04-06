package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;

import org.drools.model.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.isRegexOperator;
import static org.drools.ansible.rulebook.integration.api.domain.constraints.ListContainsConstraint.listContains;

public class Operators {

    private static final Logger log = LoggerFactory.getLogger(Operators.class);

    private static final Map<String, BiPredicate> OPERATORS_MAP = new HashMap<>();

    static {
        OPERATORS_MAP.put("==", Index.ConstraintType.EQUAL.asPredicate());
        OPERATORS_MAP.put("!=", Index.ConstraintType.NOT_EQUAL.asPredicate());
        OPERATORS_MAP.put(">", Index.ConstraintType.GREATER_THAN.asPredicate());
        OPERATORS_MAP.put(">=", Index.ConstraintType.GREATER_OR_EQUAL.asPredicate());
        OPERATORS_MAP.put("<", Index.ConstraintType.LESS_THAN.asPredicate());
        OPERATORS_MAP.put("<=", Index.ConstraintType.LESS_OR_EQUAL.asPredicate());
        OPERATORS_MAP.put("in", (a,b) -> listContains(b, a));
        OPERATORS_MAP.put("not in", (a,b) -> !listContains(b, a));
        OPERATORS_MAP.put("contains", (a,b) -> listContains(a, b));
        OPERATORS_MAP.put("not contains", (a,b) -> !listContains(a, b));
    }

    public static BiPredicate<?, ?> toOperatorPredicate(String operator) {
        if (isRegexOperator(operator)) {
            return (a,b) -> a != null && b != null && Pattern.compile(b.toString()).matcher(a.toString()).find();
        }

        BiPredicate op = OPERATORS_MAP.get(operator);
        if (op == null) {
            throw new UnsupportedOperationException("Unknown operator: " + operator);
        }
        return (a,b) -> testPredicate(a, op, b);
    }

    private static boolean testPredicate(Object left, BiPredicate op, Object right) {
        try {
            return left == null ? right == null : op.test(left, right);
        } catch (ClassCastException cce) {
            if (log.isWarnEnabled()) {
                log.warn(cce.getMessage());
            }
            return false;
        }
    }

    static boolean areEqual(Object o1, Object o2) {
        return o1 instanceof Number && o2 instanceof Number ? areNumericEqual((Number) o1, (Number) o2) : Objects.equals(o1, o2);
    }

    static <T, V> boolean areNumericEqual(Number n1, Number n2) {
        return n1.getClass() != n2.getClass() ?
                asBigDecimal(n1).equals(asBigDecimal(n2)) :
                Objects.equals(n1, n2);
    }

    static int compare(Object o1, Object o2) {
        return o1.getClass() != o2.getClass() && o1 instanceof Number && o2 instanceof Number ?
                asBigDecimal(o1).compareTo(asBigDecimal(o2)) :
                ((Comparable) o1).compareTo(o2);
    }

    static BigDecimal asBigDecimal(Object obj) {
        return obj instanceof BigDecimal ? (BigDecimal) obj : BigDecimal.valueOf(((Number) obj).doubleValue());
    }
}
