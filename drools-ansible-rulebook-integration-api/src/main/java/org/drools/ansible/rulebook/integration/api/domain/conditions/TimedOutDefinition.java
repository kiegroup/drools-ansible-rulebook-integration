package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.core.facttemplates.Event;
import org.drools.model.Drools;
import org.drools.model.DroolsEntryPoint;
import org.drools.model.Index;
import org.drools.model.Prototype;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeFact;
import org.drools.model.PrototypeVariable;
import org.drools.model.Rule;
import org.drools.model.RuleItemBuilder;
import org.drools.model.Variable;
import org.drools.model.view.ViewItem;

import static org.drools.ansible.rulebook.integration.api.RulesExecutor.SYNTHETIC_RULE_TAG;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.TimeAmount.parseTimeAmount;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.model.DSL.accFunction;
import static org.drools.model.DSL.accumulate;
import static org.drools.model.DSL.after;
import static org.drools.model.DSL.declarationOf;
import static org.drools.model.DSL.not;
import static org.drools.model.DSL.on;
import static org.drools.model.PatternDSL.pattern;
import static org.drools.model.PatternDSL.rule;
import static org.drools.model.PrototypeDSL.protoPattern;
import static org.drools.model.PrototypeDSL.prototype;
import static org.drools.model.PrototypeDSL.variable;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedEvent;

/**
 * Only a subset of the events is matched and a time out expires before all the conditions are satisfied
 *
 *    e.g.:
 *      condition:
 *         all:
 *           - events.ping << event.ping.timeout == true   # no host info
 *           - events.process << event.sensu.process.status == "stopped"   # web server
 *           - events.database << event.sensu.storage.percent > 95  # database server
 *         timed_out: 5 minutes
 *
 *  In other words this rule has to fire if at least one event is arrived by not oll inside the given time window.
 *
 *  It has been implemented by creating a single rule for each of the event in the original one. When these artificial rules
 *  fire insert a synthetic event with a lifespan equal to the one of the time window to mark the presence of that specific event.
 *  These rules also have a guard, implemented with a not, to avoid the creation of multiple synthetic events for the same rule in the time window.
 *  Those synthetic events are collected by 2 different accumulates. the first one triggers when at least one event is present
 *  and insert one further synthetic event to demarcate the start of the matching sequence. The second accumulate fires when all
 *  the expected events are arrived thus marking the end of the sequence with another synthetic events.
 *  The actual rule fires if in the time window it gets only the start of the sequence but not its end.
 *  Finally there are 2 more cleanup rule removing all those events from the working memory at the end of the time window
 *  regardless if the whole sequence has been matched completely or only partially.
 *
 *  This means that the former example is translated in the following set of rules:
 *
 *  rule R1 when
 *    ping : Event( ping.timeout == true )
 *    not( Control( name == "R1" ) )
 *  then
 *    Control control = new Control().withExpiration(5, TimeUnit.MINUTE);
 *    control.set("name", "R1");
 *    control.set("event", ping);
 *    insert(control);
 *  end
 *
 *  rule R2 when
 *    process : Event( sensu.process.status == "stopped" )
 *    not( Control( name == "R2" ) )
 *  then
 *    Control control = new Control().withExpiration(5, TimeUnit.MINUTE);
 *    control.set("name", "R2");
 *    control.set("event", process);
 *    insert(control);
 *  end
 *
 *  rule R3 when
 *    database : Event( sensu.storage.percent > 95 )
 *    not( Control( name == "R3" ) )
 *  then
 *    Control control = new Control().withExpiration(5, TimeUnit.MINUTE);
 *    control.set("name", "R3");
 *    control.set("event", database);
 *    insert(control);
 *  end
 *
 *  rule acc_start when
 *    not( Control( name == "start_R" ) )
 *    accumulate( Control( name startsWith "R" ); $count : count(); $count > 0  ) // at least one pattern
 *  then
 *    Control control = new Control().withExpiration(5, TimeUnit.MINUTE);
 *    control.set("name", "start_R");
 *    insert(control);
 *  end
 *
 *  rule acc_end when
 *    Control( name == "start_R" )
 *    accumulate( Control( name startsWith "R" ); $count : count(); $count == 3 ) // the total number of patterns
 *  then
 *    Control control = new Control().withExpiration(5, TimeUnit.MINUTE);
 *    control.set("name", "end_R");
 *    insert(control);
 *  end
 *
 *  rule check when
 *    $start : Control( name == "start_R" )
 *    not( Control( name == "end_R", this after[0m, 5m] $start ) )
 *  then
 *    // this is the actual rule to be activated and reported to users
 *  end
 *
 *  rule cleanupEvents when
 *    Control( name == "end_R" )
 *    $c : Control( name startsWith "R" )
 *  then
 *    delete($c.get("event"))
 *    delete($c)
 *  end
 *
 *  rule cleanupTerminal when
 *    $start : Control( name == "start_R" )
 *    $end : Control( name == "end_R" )
 *  then
 *    delete($start)
 *    delete($end)
 *  end
 */
public class TimedOutDefinition implements TimeConstraint {

    private final TimeAmount timeAmount;

    private final List<ViewItem> patterns = new ArrayList<>();

    private final Prototype controlPrototype = getPrototype(SYNTHETIC_PROTOTYPE_NAME);
    private final PrototypeVariable controlVar1 = variable( controlPrototype );
    private final PrototypeVariable controlVar2 = variable( controlPrototype );

    private TimedOutDefinition(TimeAmount timeAmount) {
        this.timeAmount = timeAmount;
    }

    public static TimedOutDefinition parseTimedOut(String timeWindow) {
        return new TimedOutDefinition(parseTimeAmount(timeWindow));
    }

    public ViewItem processTimeConstraint(ViewItem pattern) {
        patterns.add(pattern);
        return pattern;
    }

    @Override
    public PrototypeVariable getTimeConstraintConsequenceVariable() {
        return controlVar1;
    }

    @Override
    public BiConsumer<Drools, PrototypeFact> getTimeConstraintConsequence() {
        return Drools::delete;
    }

    @Override
    public Rule buildTimedRule(String ruleName, RuleItemBuilder pattern, RuleItemBuilder consequence) {
        String startTag = "start_" + ruleName;
        String endTag = "end_" + ruleName;

        return rule( ruleName ).build(
                protoPattern(controlVar1).expr( "name", Index.ConstraintType.EQUAL, startTag ),
                not( protoPattern(controlVar2)
                        .expr( "name", Index.ConstraintType.EQUAL, endTag )
                        .expr( after(0, timeAmount.getTimeUnit(), timeAmount.getAmount(), timeAmount.getTimeUnit()), controlVar1 ) ),
                consequence
        );
    }

    @Override
    public List<Rule> getControlRules(RuleGenerationContext ruleContext) {
        String rulePrefix = ruleContext.getRuleName() + "_";
        String startTag = "start_" + ruleContext.getRuleName();
        String endTag = "end_" + ruleContext.getRuleName();
        List<Rule> rules = new ArrayList<>();

        Variable<Long> resultCount = declarationOf( Long.class );

        for (int i = 0; i < patterns.size(); i++) {
            String name = rulePrefix + i;
            rules.add(
                rule( name ).metadata(SYNTHETIC_RULE_TAG, true)
                    .build(
                            patterns.get(i),
                            not( protoPattern(controlVar1)
                                    .expr( "name", Index.ConstraintType.EQUAL, name ) ),
                            on(patterns.get(i).getFirstVariable()).execute((drools, t1) -> {
                                Event controlEvent = createMapBasedEvent( controlPrototype )
                                        .withExpiration(timeAmount.getAmount(), timeAmount.getTimeUnit());
                                controlEvent.set( "name", name );
                                controlEvent.set( "event", t1 );
                                ((Drools) drools).insert(controlEvent);
                            })
                    )
            );
        }

        rules.add(
            rule(startTag).metadata(SYNTHETIC_RULE_TAG, true)
                .build(
                        not( protoPattern(controlVar1).expr( "name", Index.ConstraintType.EQUAL, startTag ) ),
                        accumulate( protoPattern(controlVar2).expr(p -> ((String)p.get("name")).startsWith(rulePrefix)),
                                accFunction(org.drools.core.base.accumulators.CountAccumulateFunction::new).as(resultCount)),
                        pattern(resultCount).expr(count -> count > 0),
                        on(resultCount).execute((drools, count) -> {
                            Event controlEvent = createMapBasedEvent( controlPrototype )
                                    .withExpiration(timeAmount.getAmount(), timeAmount.getTimeUnit());
                            controlEvent.set( "name", startTag );
                            drools.insert(controlEvent);
                        })
                )
        );

        rules.add(
            rule(endTag).metadata(SYNTHETIC_RULE_TAG, true)
                .build(
                        protoPattern(controlVar1).expr( "name", Index.ConstraintType.EQUAL, startTag ),
                        accumulate( protoPattern(controlVar2).expr(p -> ((String)p.get("name")).startsWith(rulePrefix)),
                                accFunction(org.drools.core.base.accumulators.CountAccumulateFunction::new).as(resultCount)),
                        pattern(resultCount).expr(count -> count == patterns.size()),
                        on(resultCount).execute((drools, count) -> {
                            Event controlEvent = createMapBasedEvent( controlPrototype )
                                    .withExpiration(timeAmount.getAmount(), timeAmount.getTimeUnit());
                            controlEvent.set( "name", endTag );
                            drools.insert(controlEvent);
                        })
                )
        );

        rules.add(
            rule( rulePrefix + "cleanupEvents" ).metadata(SYNTHETIC_RULE_TAG, true)
                .build(
                        protoPattern(controlVar1).expr( "name", Index.ConstraintType.EQUAL, endTag ),
                        protoPattern(controlVar2).expr(p -> ((String)p.get("name")).startsWith(rulePrefix)),
                        on(controlVar1, controlVar2).execute((drools, c1, c2) -> {
                            drools.delete(c2.get("event"));
                            drools.delete(c2);
                        })
                )
        );

        rules.add(
            rule( rulePrefix + "cleanupTerminal" ).metadata(SYNTHETIC_RULE_TAG, true)
                .build(
                        protoPattern(controlVar1).expr( "name", Index.ConstraintType.EQUAL, startTag ),
                        protoPattern(controlVar2).expr( "name", Index.ConstraintType.EQUAL, endTag ),
                        on(controlVar1, controlVar2).execute((drools, c1, c2) -> {
                            drools.delete(c1);
                            drools.delete(c2);
                        })
                )
        );

        return rules;
    }
}