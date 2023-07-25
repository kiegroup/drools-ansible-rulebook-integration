package org.drools.ansible.rulebook.integration.api.domain.temporal;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.rulesengine.EmptyMatchDecorator;
import org.drools.ansible.rulebook.integration.api.rulesengine.RegisterOnlyAgendaFilter;
import org.drools.ansible.rulebook.integration.protoextractor.prototype.ExtractorPrototypeExpressionUtils;
import org.drools.base.facttemplates.Event;
import org.drools.base.facttemplates.Fact;
import org.drools.model.Drools;
import org.drools.model.Index;
import org.drools.model.Prototype;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeVariable;
import org.drools.model.Rule;
import org.drools.model.RuleItemBuilder;
import org.drools.model.Variable;
import org.drools.model.view.ViewItem;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.drools.ansible.rulebook.integration.api.domain.temporal.TimeAmount.parseTimeAmount;
import static org.drools.ansible.rulebook.integration.api.rulesengine.RegisterOnlyAgendaFilter.RULE_TYPE_TAG;
import static org.drools.ansible.rulebook.integration.api.rulesengine.RegisterOnlyAgendaFilter.SYNTHETIC_RULE_TAG;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.writeMetaDataOnEvent;
import static org.drools.model.DSL.accFunction;
import static org.drools.model.DSL.accumulate;
import static org.drools.model.DSL.declarationOf;
import static org.drools.model.DSL.not;
import static org.drools.model.DSL.on;
import static org.drools.model.PatternDSL.rule;
import static org.drools.model.PrototypeDSL.protoPattern;
import static org.drools.model.PrototypeDSL.variable;
import static org.drools.model.PrototypeExpression.fixedValue;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedEvent;

/**
 * Collects and groups events within a time window. The rule fires only once at the end of the time window with a list of
 * all the unique events arrived in that window. The uniqueness criteria for the event must be specified via an additional attribute.
 *
 *    e.g.:
 *      condition:
 *         all:
 *           - singleton << event.sensu.process.type == "alert"
 *         throttle:
 *           once_after: 10 minutes
 *           group_by_attributes:
 *             - event.sensu.host
 *             - event.sensu.process.type
 *
 * It has been implemented by inserting a synthetic event for any new event not seen before in that window (according to the uniqueness criteria).
 * When the first of this synthetic event is created, another control rule inserts 2 more synthetic events: one to mark the start of the window
 * that expires at its end plus another one without an automatic expiration. Then, when the start window event expires it means that the once_after
 * window is terminated, thus it accumulates all the unique events in a list retracting both them and the other synthetic event without expiration.
 *
 * In other words the former example is translated into the following rules:
 *
 * rule R_control when
 *   e : Event( sensu.process.type == "alert" )
 *   not( Control( sensu.host == e.sensu.host, sensu.process.type == e.sensu.process.type, drools_rule_name == "R" ) )
 * then
 *   Control control = new Control();
 *   control.set("sensu.host", e.sensu.host);
 *   control.set("sensu.process.type", e.sensu.process.type);
 *   control.set("event", e);
 *   control.set("drools_rule_name", "R");
 *   insert(control);
 *   delete(e);
 * end
 *
 * rule R_start when
 *   c1 : Control( drools_rule_name == "R" )
 *   not( Control( end_once_after == "R" ) )
 * then
 *   Control startControlEvent = new Control().withExpiration(10, TimeUnit.MINUTE);
 *   startControlEvent.set("start_once_after", "R");
 *   insert(startControlEvent);
 *   Control endControlEvent = new Control();
 *   endControlEvent.set("end_once_after", "R");
 *   insert(endControlEvent);
 * end
 *
 * rule R when
 *   c1 : Control( end_once_after == "R" )
 *   not( Control( start_once_after == "R" ) )
 *   accumulate( Control( drools_rule_name == "R" ); $result : collectList() )
 * then
 *   delete(c1);
 *   // traverses all control events, deleting them and collecting the related events
 *   $result.setValue(results.stream().peek(drools::delete).map(r -> ((PrototypeFact) r).get("event")).collect(Collectors.toList()));
 * end
 *
 * rule R_cleanup_duplicate when
 *   e : Event( sensu.process.type == "alert" )
 *   c1 : Control( sensu.host == e.sensu.host, sensu.process.type == e.sensu.process.type, drools_rule_name == "R" ) )
 * then
 *   delete(e);
 * end
 */
public class OnceAfterDefinition extends OnceAbstractTimeConstraint {

    protected static final Logger log = LoggerFactory.getLogger(OnceAfterDefinition.class);

    public static final String KEYWORD = "once_after";

    private final Prototype controlPrototype = getPrototype(SYNTHETIC_PROTOTYPE_NAME);
    private final PrototypeVariable controlVar1 = variable( controlPrototype, "c1" );
    private final PrototypeVariable controlVar2 = variable( controlPrototype, "c2" );
    private final Variable<List> resultsVar = declarationOf( List.class, "results" );

    static {
        RegisterOnlyAgendaFilter.registerMatchTransformer(KEYWORD, OnceAfterDefinition::transformOnceAfterMatch);
    }

    private static Match transformOnceAfterMatch(Match match) {
        EmptyMatchDecorator rewrittenMatch = new EmptyMatchDecorator(match);
        Collection<Fact> results = ((Collection<Fact>) match.getDeclarationValue("results"));
        if (results.size() == 1) {
            rewrittenMatch.withBoundObject("m", controlFact2Event(results.iterator().next()));
        } else {
            int i = 0;
            for (Fact fact : results) {
                rewrittenMatch.withBoundObject("m_" + i++, controlFact2Event(fact));
            }
        }
        return rewrittenMatch;
    }

    private static Object controlFact2Event(Fact fact) {
        Map ruleEngineMeta = new HashMap();
        ruleEngineMeta.put("once_after_time_window", fact.get("once_after_time_window"));
        ruleEngineMeta.put("events_in_window", fact.get("events_in_window"));
        return writeMetaDataOnEvent((Fact) fact.get("event"), ruleEngineMeta);
    }

    public OnceAfterDefinition(TimeAmount timeAmount, List<GroupByAttribute> groupByAttributes) {
        super(timeAmount, groupByAttributes);
    }

    @Override
    public boolean requiresAsyncExecution() {
        return true;
    }

    @Override
    public Variable<?>[] getTimeConstraintConsequenceVariables() {
        return new Variable[] { controlVar1, resultsVar };
    }

    @Override
    public void executeTimeConstraintConsequence(Drools drools, Object... facts) {
        drools.delete(facts[0]);
        ((List) facts[1]).forEach(drools::delete);
    }

    @Override
    public ViewItem processTimeConstraint(String ruleName, ViewItem pattern) {
        this.ruleName = ruleName;
        if (guardedPattern != null) {
            throw new IllegalStateException("Cannot process this TimeConstraint twice");
        }
        guardedPattern = (PrototypeDSL.PrototypePatternDef) pattern;
        return pattern;
    }

    @Override
    public Rule buildTimedRule(String ruleName, RuleItemBuilder pattern, RuleItemBuilder consequence) {
        PrototypeVariable controlVar3 = variable( controlPrototype, "c3" );
        return rule( ruleName ).metadata(RULE_TYPE_TAG, KEYWORD)
                .build(
                        protoPattern(controlVar1).expr( ExtractorPrototypeExpressionUtils.prototypeFieldExtractor("end_once_after"), Index.ConstraintType.EQUAL, fixedValue(ruleName) ),
                        not( protoPattern(controlVar2).expr( ExtractorPrototypeExpressionUtils.prototypeFieldExtractor("start_once_after"), Index.ConstraintType.EQUAL, fixedValue(ruleName) ) ),
                        accumulate( protoPattern(controlVar3).expr(ExtractorPrototypeExpressionUtils.prototypeFieldExtractor("drools_rule_name"), Index.ConstraintType.EQUAL, fixedValue(ruleName) ),
                                accFunction(org.drools.core.base.accumulators.CollectListAccumulateFunction::new, controlVar3).as(resultsVar)),
                        consequence
                );
    }

    @Override
    public List<Rule> getControlRules(RuleGenerationContext ruleContext) {
        List<Rule> rules = new ArrayList<>();

        rules.add(
                rule(ruleName + "_control").metadata(SYNTHETIC_RULE_TAG, true)
                        .build(
                                guardedPattern,
                                not( createControlPattern() ),
                                on(getPatternVariable()).execute((drools, event) -> {
                                    Event controlEvent = createMapBasedEvent( controlPrototype );
                                    for (GroupByAttribute unique : groupByAttributes) {
                                        controlEvent.set(unique.getKey(), unique.evalExtractorOnFact(event));
                                    }
                                    controlEvent.set("drools_rule_name", ruleName);
                                    controlEvent.set( "event", event );
                                    controlEvent.set( "once_after_time_window", timeAmount.toString() );
                                    controlEvent.set( "events_in_window", 1 );
                                    drools.insert(controlEvent);
                                    drools.delete(event);
                                })
                        )
        );

        rules.add(
                rule(ruleName + "_start").metadata(SYNTHETIC_RULE_TAG, true)
                        .build(
                                protoPattern(controlVar1).expr( ExtractorPrototypeExpressionUtils.prototypeFieldExtractor("drools_rule_name"), Index.ConstraintType.EQUAL, fixedValue(ruleName) ),
                                not( protoPattern(controlVar2).expr( ExtractorPrototypeExpressionUtils.prototypeFieldExtractor("end_once_after"), Index.ConstraintType.EQUAL, fixedValue(ruleName) ) ),
                                on(controlVar1).execute((drools, c1) -> {
                                    Event startControlEvent = createMapBasedEvent( controlPrototype )
                                            .withExpiration(timeAmount.getAmount(), timeAmount.getTimeUnit());
                                    startControlEvent.set( "start_once_after", ruleName );
                                    drools.insert(startControlEvent);

                                    Event endControlEvent = createMapBasedEvent( controlPrototype );
                                    endControlEvent.set( "end_once_after", ruleName );
                                    drools.insert(endControlEvent);

                                    if (log.isInfoEnabled()) {
                                        log.info("Start once_after window for rule " + ruleName);
                                    }
                                })
                        )
        );

        rules.add(
                rule(ruleName + "_cleanup_duplicate").metadata(SYNTHETIC_RULE_TAG, true)
                        .build(
                                guardedPattern,
                                createControlPattern(),
                                on(getPatternVariable(), getControlVariable()).execute((drools, event, control) -> {
                                    control.set( "events_in_window", ((int) control.get("events_in_window")) + 1 );
                                    drools.delete(event);
                                })
                        )
        );

        return rules;
    }

    @Override
    public String toString() {
        return "OnceWithinDefinition{" + " " + timeAmount + ", groupByAttributes=" + groupByAttributes + " }";
    }

    public static OnceAfterDefinition parseOnceAfter(String onceWithin, List<String> groupByAttributes) {
        List<GroupByAttribute> sanitizedAttributes = groupByAttributes.stream()
                .map(OnceAbstractTimeConstraint::sanitizeAttributeName)
                .map(GroupByAttribute::from)
                .collect(toList());
        return new OnceAfterDefinition(parseTimeAmount(onceWithin), sanitizedAttributes);
    }
}
