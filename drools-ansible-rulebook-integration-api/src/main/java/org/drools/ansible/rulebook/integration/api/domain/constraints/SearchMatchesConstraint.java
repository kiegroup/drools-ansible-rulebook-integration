package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.ConstraintOperator;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.map2Expr;
import static org.drools.model.PrototypeExpression.fixedValue;

public enum SearchMatchesConstraint implements ConstraintOperator, ConditionFactory {

    INSTANCE;

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "SEARCH_MATCHES";
    }

    @Override
    public ParsedCondition createParsedCondition(RuleGenerationContext ruleContext, String expressionName, Map<?, ?> expression) {
        ConditionExpression left = map2Expr(ruleContext, expression.get("lhs"));
        ConstraintOperator operator = createMatchOperator((Map<?,?>)expression.get("rhs"));
        return new ParsedCondition(left.getPrototypeExpression(), operator, fixedValue(true));
    }

    public static ConstraintOperator createMatchOperator(Map<?,?> rhs) {
        Map searchType = (Map)rhs.get("SearchType");
        return new RegexConstraint(parsePattern(searchType), parseOptions(searchType));
    }

    private static String parsePattern(Map searchType) {
        String kind = ((Map) searchType.get("kind")).get("String").toString();
        String pattern = ((Map) searchType.get("pattern")).get("String").toString();
        if (kind.equals("match")) {
            pattern = pattern + ".*";
        } else if (kind.equals("search") || kind.equals("regex")) {
            pattern = ".*" + pattern + ".*";
        } else {
            throw new UnsupportedOperationException("Unknown kind: " + kind);
        }
        return pattern;
    }

    private static int parseOptions(Map searchType) {
        int flags = 0;
        List<Map> options = (List<Map>) searchType.get("options");
        if (options != null) {
            for (Map option : options) {
                String optionName = ((Map) option.get("name")).get("String").toString();
                String optionValue = ((Map) option.get("value")).get("Boolean").toString();
                if (optionValue.equalsIgnoreCase("true")) {
                    switch (optionName) {
                        case "ignorecase":
                            flags += Pattern.CASE_INSENSITIVE;
                            break;
                        case "multiline":
                            flags += Pattern.MULTILINE;
                            break;
                        default:
                            throw new UnsupportedOperationException("Unknown option: " + optionName);
                    }
                }
            }
        }
        return flags;
    }

    public static class RegexConstraint implements ConstraintOperator {

        private final Pattern regexPattern;

        public RegexConstraint(String pattern, int flags) {
            this.regexPattern = Pattern.compile(pattern, flags);
        }

        @Override
        public <T, V> BiPredicate<T, V> asPredicate() {
            return (t, v) -> t != null && regexPattern.matcher(t.toString()).matches();
        }
    }
}