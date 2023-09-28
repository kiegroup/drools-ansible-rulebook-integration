package org.drools.ansible.rulebook.integration.api.domain.constraints;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.ConstraintOperator;

import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.map2Expr;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.isRegexOperator;
import static org.drools.model.PrototypeExpression.fixedValue;

public enum SearchMatchesConstraint implements RulebookOperator, ConditionFactory {

    INSTANCE;

    public static final String EXPRESSION_NAME = "SearchMatchesExpression";
    public static final String NEGATED_EXPRESSION_NAME = "SearchNotMatchesExpression";

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
        ConditionExpression lhs = map2Expr(ruleContext, expression.get("lhs"));
        Map searchType = (Map) ((Map)expression.get("rhs")).get("SearchType");
        boolean positive = expressionName.equals(EXPRESSION_NAME);

        return lhs.isFixedValue() ?
                createConditionWithFixedLeft(ruleContext, lhs.getFixedValue().toString(), searchType, positive) :
                createConditionWithFixedRight(lhs, searchType, positive);
    }

    private ParsedCondition createConditionWithFixedRight(ConditionExpression lhs, Map searchType, boolean positive) {
        int options = parseOptions(searchType);
        String pattern = ((Map) searchType.get("pattern")).get("String").toString();
        ConstraintOperator operator = new RegexConstraint(getPatternTransformerForKind(searchType, options).apply(pattern), options);
        return new ParsedCondition(lhs.getPrototypeExpression(), operator, fixedValue(positive));
    }

    private ParsedCondition createConditionWithFixedLeft(RuleGenerationContext ruleContext, String pattern, Map searchType, boolean positive) {
        int options = parseOptions(searchType);
        ConstraintOperator operator = new InvertedRegexConstraint(pattern, options, getPatternTransformerForKind(searchType, options));
        return new ParsedCondition(map2Expr(ruleContext, searchType.get("pattern")).getPrototypeExpression(), operator, fixedValue(positive));
    }

    private static UnaryOperator<String> getPatternTransformerForKind(Map searchType, int options) {
        String kind = ((Map) searchType.get("kind")).get("String").toString();
        if (!isRegexOperator(kind)) {
            throw new UnsupportedOperationException("Unknown kind: " + kind);
        }
        if (kind.equals("match")) {
            return pattern -> ((options | Pattern.MULTILINE) != 0 ? "^" : "\\A") + pattern;
        }
        return UnaryOperator.identity();
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
            return (t, v) -> t != null && regexPattern.matcher(t.toString()).find() == (boolean) v;
        }
    }

    public static class InvertedRegexConstraint implements ConstraintOperator {

        private final String pattern;
        private final int flags;
        private final UnaryOperator<String> patternTransformer;

        public InvertedRegexConstraint(String pattern, int flags, UnaryOperator<String> patternTransformer) {
            this.pattern = pattern;
            this.flags = flags;
            this.patternTransformer = patternTransformer;
        }

        @Override
        public <T, V> BiPredicate<T, V> asPredicate() {
            return (t, v) -> t != null && Pattern.compile(patternTransformer.apply(t.toString()), flags).matcher(pattern).find() == (boolean) v;
        }
    }
}