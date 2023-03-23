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

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.MapCondition;
import org.drools.model.PatternDSL;
import org.drools.model.view.CombinedExprViewItem;
import org.drools.model.view.ViewItem;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MapConditionToPatternBindingTest extends ToPatternTestBase {

    @Test
    public void assignmentExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createLhsForEventField("abc");
        LinkedHashMap<Object, Object> equalsExpression = createEqualsExpression(createSingleMap("Event", "i"), createSingleMap("Integer", 1));
        LinkedHashMap<Object, Object> assignmentExpression = createAssignmentExpression(lhsValueMap, equalsExpression);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(assignmentExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern
        RuleGenerationContext ruleGenerationContext = new RuleGenerationContext();
        ruleGenerationContext.setCondition(mapCondition);
        PatternDSL.PatternDefImpl viewItem = (PatternDSL.PatternDefImpl) mapCondition.toPattern(ruleGenerationContext);

        assertThat(viewItem.getFirstVariable().getName()).isEqualTo("abc");
    }

    @Test
    public void defaultBinding_oneCondition() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> equalsExpression = createEqualsExpression(createSingleMap("Event", "i"), createSingleMap("Integer", 1));
        LinkedHashMap<Object, Object> rootMap = createAllCondition(equalsExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern
        RuleGenerationContext ruleGenerationContext = new RuleGenerationContext();
        ruleGenerationContext.setCondition(mapCondition);
        PatternDSL.PatternDefImpl viewItem = (PatternDSL.PatternDefImpl) mapCondition.toPattern(ruleGenerationContext);

        assertThat(viewItem.getFirstVariable().getName()).isEqualTo("m");
    }

    @Test
    public void defaultBinding_multipleConditions() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> equalsExpression1 = createEqualsExpression(createSingleMap("Event", "i"), createSingleMap("Integer", 1));
        LinkedHashMap<Object, Object> equalsExpression2 = createEqualsExpression(createSingleMap("Event", "i"), createSingleMap("Integer", 2));
        LinkedHashMap<Object, Object> equalsExpression3 = createEqualsExpression(createSingleMap("Event", "i"), createSingleMap("Integer", 3));
        LinkedHashMap<Object, Object> rootMap = createAllCondition(equalsExpression1, equalsExpression2, equalsExpression3);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern
        RuleGenerationContext ruleGenerationContext = new RuleGenerationContext();
        ruleGenerationContext.setCondition(mapCondition);
        CombinedExprViewItem combinedExprViewItem = (CombinedExprViewItem) mapCondition.toPattern(ruleGenerationContext);

        ViewItem[] viewItems = combinedExprViewItem.getExpressions();
        assertThat(viewItems[0].getFirstVariable().getName()).isEqualTo("m_0");
        assertThat(viewItems[1].getFirstVariable().getName()).isEqualTo("m_1");
        assertThat(viewItems[2].getFirstVariable().getName()).isEqualTo("m_2");
    }
}
