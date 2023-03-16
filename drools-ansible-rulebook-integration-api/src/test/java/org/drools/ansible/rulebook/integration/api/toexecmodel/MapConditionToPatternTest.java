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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.MapCondition;
import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.drools.core.facttemplates.Event;
import org.drools.model.PatternDSL;
import org.drools.model.functions.Predicate1;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MapConditionToPatternTest {

    @Test
    public void selectExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createLhsForEventField("levels");
        LinkedHashMap<Object, Object> rhsValueMap = createRhsWithOperatorAndValue("String", ">", "Integer", 25);
        LinkedHashMap<Object, Object> selectExpression = createSelectExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(selectExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        RuleGenerationContext ruleGenerationContext = new RuleGenerationContext();
        ruleGenerationContext.setCondition(mapCondition);
        PatternDSL.PatternDefImpl viewItem = (PatternDSL.PatternDefImpl) mapCondition.toPattern(ruleGenerationContext);
        Predicate1.Impl predicate = extractFirstPredicate(viewItem);

        Event event1 = createEvent("levels", 25);
        assertThat(predicate.test(event1)).isFalse();

        Event event2 = createEvent("levels", 26);
        assertThat(predicate.test(event2)).isTrue();
    }

    private Event createEvent(String fieldName, Object fieldValue) {
        Map<String, Object> factMap = new HashMap<>();
        factMap.put(fieldName, fieldValue);
        return (Event) RulesModelUtil.mapToFact(factMap, true);
    }

    private LinkedHashMap<Object, Object> createLhsForEventField(String fieldName) {
        return createSingleMap("Event", fieldName);
    }

    private LinkedHashMap<Object, Object> createRhsWithOperatorAndValue(String operatorType, String operatorValue, String valueType, Object valueValue) {
        LinkedHashMap<Object, Object> rhsValueMap = new LinkedHashMap<>();
        rhsValueMap.put("operator", createSingleMap(operatorType, operatorValue));
        rhsValueMap.put("value", createSingleMap(valueType, valueValue));
        return rhsValueMap;
    }

    private LinkedHashMap<Object, Object> createSelectExpression(LinkedHashMap<Object, Object> lhsValueMap, LinkedHashMap<Object, Object> rhsValueMap) {
        LinkedHashMap<Object, Object> selectExpressionValueMap = new LinkedHashMap<>();
        selectExpressionValueMap.put("lhs", lhsValueMap);
        selectExpressionValueMap.put("rhs", rhsValueMap);
        return createSingleMap("SelectExpression", selectExpressionValueMap);
    }

    private LinkedHashMap<Object, Object> createAllCondition(LinkedHashMap<Object, Object> expression) { // can be varargs
        ArrayList<Object> allCondition = new ArrayList<>();
        allCondition.add(expression);
        return createSingleMap("AllCondition", allCondition);
    }

    private LinkedHashMap<Object, Object> createSingleMap(Object key, Object value) {
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        map.put(key, value);
        return map;
    }

    private Predicate1.Impl extractFirstPredicate(PatternDSL.PatternDefImpl viewItem) {
        PatternDSL.PatternExpr1 expr1 = (PatternDSL.PatternExpr1) viewItem.getItems().get(0);
        return (Predicate1.Impl) expr1.getPredicate();
    }
}
