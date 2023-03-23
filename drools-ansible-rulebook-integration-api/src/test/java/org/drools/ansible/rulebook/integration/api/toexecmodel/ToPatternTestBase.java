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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.Condition;
import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.drools.core.facttemplates.Event;
import org.drools.model.PatternDSL;
import org.drools.model.functions.Predicate1;

public class ToPatternTestBase {

    // Create an Event with single field. Multiple values is converted to a List
    protected Event createEvent(String fieldName, Object... fieldValues) {
        Object fieldValue;
        if (fieldValues.length == 1) {
            fieldValue = fieldValues[0];
        } else {
            fieldValue = Arrays.asList(fieldValues);
        }
        Map<String, Object> factMap = new HashMap<>();
        factMap.put(fieldName, fieldValue);
        return createEvent(factMap);
    }

    protected Event createNestedEvent(Map<String, Object> nestedFactMap) {
        Map<String, Object> factMap = new HashMap<>();
        factMap.put("nested", nestedFactMap);
        return createEvent(factMap);
    }

    protected Event createEvent(Map<String, Object> factMap) {
        return (Event) RulesModelUtil.mapToFact(factMap, true);
    }

    protected LinkedHashMap<Object, Object> createLhsForEventField(String fieldName) {
        return createSingleMap("Event", fieldName);
    }

    protected LinkedHashMap<Object, Object> createRhsWithOperatorAndValue(String operatorType, String operatorValue, String valueType, Object valueValue) {
        LinkedHashMap<Object, Object> rhsValueMap = new LinkedHashMap<>();
        rhsValueMap.put("operator", createSingleMap(operatorType, operatorValue));
        rhsValueMap.put("value", createSingleMap(valueType, valueValue));
        return rhsValueMap;
    }

    protected LinkedHashMap<Object, Object> createSelectExpression(LinkedHashMap<Object, Object> lhsValueMap, LinkedHashMap<Object, Object> rhsValueMap) {
        return createExpression("SelectExpression", lhsValueMap, rhsValueMap);
    }

    protected LinkedHashMap<Object, Object> createSelectNotExpression(LinkedHashMap<Object, Object> lhsValueMap, LinkedHashMap<Object, Object> rhsValueMap) {
        return createExpression("SelectNotExpression", lhsValueMap, rhsValueMap);
    }

    protected LinkedHashMap<Object, Object> createAdditionExpression(String leftKey, String leftValue, String rightKey, Object rightValue) {
        return createExpression("AdditionExpression", createSingleMap(leftKey, leftValue), createSingleMap(rightKey, rightValue));
    }

    protected LinkedHashMap<Object, Object> createEqualsExpression(LinkedHashMap<Object, Object> lhsValueMap, LinkedHashMap<Object, Object> rhsValueMap) {
        return createExpression("EqualsExpression", lhsValueMap, rhsValueMap);
    }

    protected LinkedHashMap<Object, Object> createAssignmentExpression(LinkedHashMap<Object, Object> lhsValueMap, LinkedHashMap<Object, Object> rhsValueMap) {
        return createExpression("AssignmentExpression", lhsValueMap, rhsValueMap);
    }

    protected LinkedHashMap<Object, Object> createExpression(String expressionName, LinkedHashMap<Object, Object> lhsValueMap, LinkedHashMap<Object, Object> rhsValueMap) {
        LinkedHashMap<Object, Object> expressionValueMap = new LinkedHashMap<>();
        expressionValueMap.put("lhs", lhsValueMap);
        expressionValueMap.put("rhs", rhsValueMap);
        return createSingleMap(expressionName, expressionValueMap);
    }

    protected LinkedHashMap<Object, Object> createAllCondition(LinkedHashMap<Object, Object>... expressions) {
        ArrayList<Object> allCondition = new ArrayList<>(Arrays.asList(expressions));
        return createSingleMap("AllCondition", allCondition);
    }

    protected LinkedHashMap<Object, Object> createSingleMap(Object key, Object value) {
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        map.put(key, value);
        return map;
    }

    protected Predicate1.Impl toPatternAndGetFirstPredicate(Condition condition) {
        RuleGenerationContext ruleGenerationContext = new RuleGenerationContext();
        ruleGenerationContext.setCondition(condition);
        PatternDSL.PatternDefImpl viewItem = (PatternDSL.PatternDefImpl) condition.toPattern(ruleGenerationContext);
        return extractFirstPredicate(viewItem);
    }

    protected Predicate1.Impl extractFirstPredicate(PatternDSL.PatternDefImpl viewItem) {
        PatternDSL.PatternExpr1 expr1 = (PatternDSL.PatternExpr1) viewItem.getItems().get(0);
        return (Predicate1.Impl) expr1.getPredicate();
    }
}
