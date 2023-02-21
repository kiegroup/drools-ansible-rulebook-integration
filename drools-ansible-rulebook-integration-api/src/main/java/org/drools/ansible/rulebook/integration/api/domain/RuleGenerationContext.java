package org.drools.ansible.rulebook.integration.api.domain;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleConfigurationOptions;
import org.drools.ansible.rulebook.integration.api.domain.actions.Action;
import org.drools.ansible.rulebook.integration.api.domain.conditions.Condition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeConstraint;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutionController;
import org.drools.model.Drools;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeVariable;
import org.drools.model.Rule;
import org.drools.model.RuleItemBuilder;
import org.drools.model.Variable;
import org.drools.model.view.ViewItem;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.model.DSL.execute;
import static org.drools.model.DSL.on;
import static org.drools.model.PatternDSL.rule;
import static org.drools.model.PrototypeDSL.protoPattern;
import static org.drools.model.PrototypeDSL.variable;

public class RuleGenerationContext {

    private final RuleConfigurationOptions options = new RuleConfigurationOptions();

    private final StackedContext<String, PrototypeDSL.PrototypePatternDef> patterns = new StackedContext<>();

    private String ruleName;

    private int bindingsCounter = 0;

    private boolean multiplePatterns = false;

    private Condition condition;

    private Action action;

    private ViewItem lhs;

    private TimeConstraint timeConstraint;

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public Action getAction() {
        return action;
    }

    public void addOptions(Iterable<RuleConfigurationOption> options) {
        this.options.addOptions(options);
    }

    public PrototypeDSL.PrototypePatternDef getOrCreatePattern(String binding, String name) {
        return patterns.computeIfAbsent(binding, b -> protoPattern(variable(getPrototype(name), b)));
    }

    public PrototypeVariable getPatternVariable(String binding) {
        PrototypeDSL.PrototypePatternDef patternDef = patterns.get(binding);
        return patternDef != null ? (PrototypeVariable) patternDef.getFirstVariable() : null;
    }

    public boolean isExistingBoundVariable(String binding) {
        return patterns.get(binding) != null;
    }

    public PrototypeDSL.PrototypePatternDef getBoundPattern(String binding) {
        return patterns.get(binding);
    }

    public void pushContext() {
        patterns.pushContext();
    }

    public void popContext() {
        patterns.popContext();
    }

    public void setMultiplePatterns(boolean multiplePatterns) {
        this.multiplePatterns = multiplePatterns;
    }

    public String generateBinding() {
        String binding = ( !multiplePatterns && bindingsCounter == 0 ) ? "m" : "m_" + bindingsCounter;
        bindingsCounter++;
        return binding;
    }

    public boolean hasOption(RuleConfigurationOption option) {
        return options.hasOption(option);
    }

    public static boolean isGeneratedBinding(String binding) {
        return binding.equals("m") || binding.startsWith("m_");
    }

    public Optional<TimeConstraint> getTimeConstraint() {
        return Optional.ofNullable(timeConstraint);
    }

    public void setTimeConstraint(TimeConstraint timeConstraint) {
        if (this.timeConstraint != null) {
            throw new IllegalArgumentException("Cannot add more than one time constraint to the same rule");
        }
        this.timeConstraint = timeConstraint;
    }

    public boolean hasTemporalConstraint() {
        getOrCreateLHS();
        return timeConstraint != null;
    }

    public boolean requiresAsyncExecution(org.drools.ansible.rulebook.integration.api.domain.Rule rule) {
    	updateContextFromRule(rule);
        getOrCreateLHS();
        return timeConstraint != null && timeConstraint.requiresAsyncExecution();
    }

    public List<Rule> createRules(RulesExecutionController rulesExecutionController) {
        Rule generatedRule = createRule(rulesExecutionController);
        List<org.drools.model.Rule> syntheticRules = getSyntheticRules();

        if (syntheticRules.isEmpty()) {
            return Collections.singletonList(generatedRule);
        }

        List<org.drools.model.Rule> rules = new ArrayList<>();
        rules.add(generatedRule);
        rules.addAll(syntheticRules);
        return rules;
    }

    private Rule createRule(RulesExecutionController rulesExecutionController) {
        RuleItemBuilder pattern = getOrCreateLHS();
        RuleItemBuilder consequence = createConsequence(rulesExecutionController, action);

        return getTimeConstraint().map( tc -> tc.buildTimedRule(ruleName, pattern, consequence) )
                .orElse(rule( ruleName ).build( pattern, consequence ));
    }

    private ViewItem getOrCreateLHS() {
        if (lhs == null) {
            lhs = condition.toPattern(this);
        }
        return lhs;
    }

    private RuleItemBuilder createConsequence(RulesExecutionController rulesExecutionController, Action action) {
        RuleItemBuilder consequence;
        Variable<?>[] consequenceVariables = timeConstraint != null ? timeConstraint.getTimeConstraintConsequenceVariables() : null;
        if ( consequenceVariables != null ) {
            if (consequenceVariables.length == 1) {
                consequence = on(consequenceVariables[0]).execute((drools, fact) -> {
                    timeConstraint.executeTimeConstraintConsequence(drools, fact);
                    defaultConsequence(rulesExecutionController, action, drools);
                });
            } else if (consequenceVariables.length == 2) {
                consequence = on(consequenceVariables[0], consequenceVariables[1]).execute((drools, fact1, fact2) -> {
                    timeConstraint.executeTimeConstraintConsequence(drools, fact1, fact2);
                    defaultConsequence(rulesExecutionController, action, drools);
                });
            } else {
                throw new UnsupportedOperationException();
            }
        } else {
            consequence = execute(drools -> defaultConsequence(rulesExecutionController, action, drools));
        }
        return consequence;
    }

    private void defaultConsequence(RulesExecutionController rulesExecutionController, Action action, Drools drools) {
        if (rulesExecutionController.executeActions()) {
            action.execute(drools);
        }
    }

    private List<Rule> getSyntheticRules() {
        return timeConstraint != null ? timeConstraint.getControlRules(this) : Collections.emptyList();
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    List<Rule> toExecModelRules(RulesSet rulesSet, org.drools.ansible.rulebook.integration.api.domain.Rule ansibleRule, RulesExecutionController rulesExecutionController, AtomicInteger ruleCounter) {
    	updateContextFromRule(ansibleRule);
	    if (getRuleName() == null) {
	        setRuleName("r_" + ruleCounter.getAndIncrement());
	    }
	
	    List<org.drools.model.Rule> rules = createRules(rulesExecutionController);
	    if (hasTemporalConstraint()) {
	        rulesSet.withOptions(RuleConfigurationOption.EVENTS_PROCESSING);
	    }
	    return rules;
	}

	private void updateContextFromRule(org.drools.ansible.rulebook.integration.api.domain.Rule anisbleRule) {
		setRuleName(anisbleRule.getName());
		setAction(anisbleRule.getAction());
		if (anisbleRule.getOptions() != null) {
			addOptions(anisbleRule.getOptions().getOptions());
		}
		setCondition(anisbleRule.getCondition());
	}

	private static class StackedContext<K, V> {
        private final Deque<Map<K, V>> stack = new ArrayDeque<>();

        public StackedContext() {
            pushContext();
        }

        public void pushContext() {
            stack.addFirst(new HashMap<>());
        }

        public void popContext() {
            stack.removeFirst();
        }

        public V get(K key) {
            for (Map<K,V> map : stack) {
                V value = map.get(key);
                if (value != null) {
                    return value;
                }
            }
            return null;
        }

        public void put(K key, V value) {
            stack.getFirst().put(key, value);
        }

        public V computeIfAbsent(K key, Function<K, V> f) {
            V value = get(key);
            if (value != null) {
                return value;
            }
            value = f.apply(key);
            put(key, value);
            return value;
        }
    }
}
