/*
 * Copyright 2023 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.drools.ansible.rulebook.integration.api.toexecmodel;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.drools.ansible.rulebook.integration.api.domain.Rule;
import org.drools.ansible.rulebook.integration.api.domain.RuleContainer;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.domain.actions.MapAction;
import org.drools.ansible.rulebook.integration.api.domain.conditions.MapCondition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.OnceAfterDefinition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.Throttle;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutionController;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.rulesengine.RegisterOnlyAgendaFilter.RULE_TYPE_TAG;
import static org.drools.ansible.rulebook.integration.api.rulesengine.RegisterOnlyAgendaFilter.SYNTHETIC_RULE_TAG;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.createEventField;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.createSingleMap;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.getRuleByName;

public class ToExecModelRulesOnceAfterTest extends ToPatternTestBase {

    @Test
    public void onceAfter() throws Exception {
        // Rule
        Rule rule = new Rule();
        rule.setName("r1");

        // Condition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("alert.level");
        LinkedHashMap<Object, Object> rhsValueMap = createSingleMap("String", "warning");
        LinkedHashMap<Object, Object> equalsExpression = createEqualsExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(equalsExpression);
        MapCondition mapCondition = new MapCondition(rootMap);
        rule.setCondition(mapCondition);

        // Action
        LinkedHashMap<Object, Object> actionDetails = createSingleMap("action", "print_event");
        MapAction mapAction = new MapAction();
        mapAction.put("Action", actionDetails);
        rule.setAction(mapAction);

        // OnceAfter
        Throttle throttle = new Throttle();
        throttle.setOnce_after("10 seconds");
        throttle.setGroup_by_attributes(List.of("event.meta.host", "event.alert.level"));
        rule.setThrottle(throttle);

        // RulesSet
        RulesSet rulesSet = new RulesSet();
        RuleContainer ruleContainer = new RuleContainer();
        ruleContainer.setRule(rule);
        rulesSet.setRules(List.of(ruleContainer));

        RulesExecutionController rulesExecutionController = new RulesExecutionController();
        AtomicInteger ruleCounter = new AtomicInteger(0);

        List<org.drools.model.Rule> rules = rule.toExecModelRules(rulesSet, rulesExecutionController, ruleCounter);

        assertThat(rules).as("OnceAfter generates additional 3 synthetic rules").hasSize(4);

        // r1
        org.drools.model.Rule r1 = getRuleByName(rules, "r1");
        assertThat(r1).isNotNull();
        assertThat(r1.getMetaData(RULE_TYPE_TAG)).isEqualTo(OnceAfterDefinition.KEYWORD);

        // r1_control
        org.drools.model.Rule r1_control = getRuleByName(rules, "r1_control");
        assertThat(r1_control).isNotNull();
        assertThat(r1_control.getMetaData(SYNTHETIC_RULE_TAG)).isEqualTo(true);

        // r1_start
        org.drools.model.Rule r1_start = getRuleByName(rules, "r1_start");
        assertThat(r1_start).isNotNull();
        assertThat(r1_control.getMetaData(SYNTHETIC_RULE_TAG)).isEqualTo(true);

        // r1_cleanup_duplicate
        org.drools.model.Rule r1_cleanup_duplicate = getRuleByName(rules, "r1_cleanup_duplicate");
        assertThat(r1_cleanup_duplicate).isNotNull();
        assertThat(r1_control.getMetaData(SYNTHETIC_RULE_TAG)).isEqualTo(true);
    }
}
