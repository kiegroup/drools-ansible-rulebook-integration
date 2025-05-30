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

import org.drools.ansible.rulebook.integration.api.domain.conditions.MapCondition;
import org.drools.model.Constraint;
import org.drools.model.constraints.OrConstraints;
import org.drools.model.functions.Predicate1;
import org.junit.jupiter.api.Test;
import org.kie.api.prototype.PrototypeEventInstance;

import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.createEventField;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.createSingleMap;

public class MapConditionToPatternLogicalOperatorsTest extends ToPatternTestBase {

    @Test
    void andExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap1 = createEventField("i");
        LinkedHashMap<Object, Object> rhsValueMap1 = createSingleMap("Integer", "2");
        LinkedHashMap<Object, Object> equalsExpression1 = createEqualsExpression(lhsValueMap1, rhsValueMap1);
        LinkedHashMap<Object, Object> lhsValueMap2 = createEventField("j");
        LinkedHashMap<Object, Object> rhsValueMap2 = createSingleMap("Integer", "2");
        LinkedHashMap<Object, Object> equalsExpression2 = createEqualsExpression(lhsValueMap2, rhsValueMap2);
        LinkedHashMap<Object, Object> andExpression = createAndExpression(equalsExpression1, equalsExpression2);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(andExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        PrototypeEventInstance event1 = createIJEvent(1, 1);
        assertThat(predicate.test(event1)).isFalse();

        PrototypeEventInstance event2 = createIJEvent(1, 2);
        assertThat(predicate.test(event2)).isFalse();

        PrototypeEventInstance event3 = createIJEvent(2, 2);
        assertThat(predicate.test(event3)).isTrue();
    }

    @Test
    void orExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap1 = createEventField("i");
        LinkedHashMap<Object, Object> rhsValueMap1 = createSingleMap("Integer", "2");
        LinkedHashMap<Object, Object> equalsExpression1 = createEqualsExpression(lhsValueMap1, rhsValueMap1);
        LinkedHashMap<Object, Object> lhsValueMap2 = createEventField("j");
        LinkedHashMap<Object, Object> rhsValueMap2 = createSingleMap("Integer", "2");
        LinkedHashMap<Object, Object> equalsExpression2 = createEqualsExpression(lhsValueMap2, rhsValueMap2);
        LinkedHashMap<Object, Object> orExpression = createOrExpression(equalsExpression1, equalsExpression2);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(orExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its constraint
        Constraint constraint = toPatternAndGetRootConstraint(mapCondition);
        assertThat(constraint).isInstanceOf(OrConstraints.class);

        Predicate1.Impl firstPredicate = extractFirstPredicate((OrConstraints) constraint);
        Predicate1.Impl secondPredicate = extractSecondPredicate((OrConstraints) constraint);

        PrototypeEventInstance event1 = createIJEvent(1, 1);
        assertThat(firstPredicate.test(event1)).isFalse();
        assertThat(secondPredicate.test(event1)).isFalse();

        PrototypeEventInstance event2 = createIJEvent(1, 2);
        assertThat(firstPredicate.test(event2)).isFalse();
        assertThat(secondPredicate.test(event2)).isTrue();

        PrototypeEventInstance event3 = createIJEvent(2, 2);
        assertThat(firstPredicate.test(event3)).isTrue();
        assertThat(secondPredicate.test(event3)).isTrue();
    }
}
