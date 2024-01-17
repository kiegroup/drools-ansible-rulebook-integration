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
import org.drools.model.functions.Predicate1;
import org.junit.Test;
import org.kie.api.prototype.PrototypeEventInstance;

import java.math.BigDecimal;
import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.createEventField;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.createSingleMap;

public class MapConditionToPatternRelationalOperatorsTest extends ToPatternTestBase {

    @Test
    public void equalsExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("i");
        LinkedHashMap<Object, Object> rhsValueMap = createEventField("j");
        LinkedHashMap<Object, Object> equalsExpression = createEqualsExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(equalsExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        PrototypeEventInstance event1 = createIJEvent(1, 2);
        assertThat(predicate.test(event1)).isFalse();

        PrototypeEventInstance event2 = createIJEvent(2, 2);
        assertThat(predicate.test(event2)).isTrue();
    }

    @Test
    public void equalsExpression_BigDecimalDifferentScale() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("i");
        LinkedHashMap<Object, Object> rhsValueMap = createSingleMap("Integer", 1.0); // actually, this will be BigDecimal("1.0")
        LinkedHashMap<Object, Object> equalsExpression = createEqualsExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(equalsExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        PrototypeEventInstance event1 = createIEvent(0);
        assertThat(predicate.test(event1)).isFalse();

        PrototypeEventInstance event2 = createIEvent(1);
        assertThat(predicate.test(event2)).isTrue();

        PrototypeEventInstance event3 = createIEvent(new BigDecimal("1.0"));
        assertThat(predicate.test(event3)).isTrue();

        PrototypeEventInstance event4 = createIEvent(new BigDecimal("1.00")); // different scale
        assertThat(predicate.test(event4)).isTrue();
    }

    @Test
    public void notEqualsExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("i");
        LinkedHashMap<Object, Object> rhsValueMap = createEventField("j");
        LinkedHashMap<Object, Object> notEqualsExpression = createNotEqualsExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(notEqualsExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        PrototypeEventInstance event1 = createIJEvent(1, 2);
        assertThat(predicate.test(event1)).isTrue();

        PrototypeEventInstance event2 = createIJEvent(2, 2);
        assertThat(predicate.test(event2)).isFalse();
    }

    @Test
    public void greaterThanExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("i");
        LinkedHashMap<Object, Object> rhsValueMap = createEventField("j");
        LinkedHashMap<Object, Object> greaterThanExpression = createGreaterThanExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(greaterThanExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        PrototypeEventInstance event1 = createIJEvent(1, 2);
        assertThat(predicate.test(event1)).isFalse();

        PrototypeEventInstance event2 = createIJEvent(2, 2);
        assertThat(predicate.test(event2)).isFalse();

        PrototypeEventInstance event3 = createIJEvent(2, 1);
        assertThat(predicate.test(event3)).isTrue();
    }

    @Test
    public void greaterThanOrEqualToExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("i");
        LinkedHashMap<Object, Object> rhsValueMap = createEventField("j");
        LinkedHashMap<Object, Object> greaterThanOrEqualToExpression = createGreaterThanOrEqualToExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(greaterThanOrEqualToExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        PrototypeEventInstance event1 = createIJEvent(1, 2);
        assertThat(predicate.test(event1)).isFalse();

        PrototypeEventInstance event2 = createIJEvent(2, 2);
        assertThat(predicate.test(event2)).isTrue();

        PrototypeEventInstance event3 = createIJEvent(2, 1);
        assertThat(predicate.test(event3)).isTrue();
    }

    @Test
    public void lessThanExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("i");
        LinkedHashMap<Object, Object> rhsValueMap = createEventField("j");
        LinkedHashMap<Object, Object> lessThanExpression = createLessThanExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(lessThanExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        PrototypeEventInstance event1 = createIJEvent(1, 2);
        assertThat(predicate.test(event1)).isTrue();

        PrototypeEventInstance event2 = createIJEvent(2, 2);
        assertThat(predicate.test(event2)).isFalse();

        PrototypeEventInstance event3 = createIJEvent(2, 1);
        assertThat(predicate.test(event3)).isFalse();
    }

    @Test
    public void lessThanOrEqualToExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("i");
        LinkedHashMap<Object, Object> rhsValueMap = createEventField("j");
        LinkedHashMap<Object, Object> lessThanOrEqualToExpression = createLessThanOrEqualToExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(lessThanOrEqualToExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        PrototypeEventInstance event1 = createIJEvent(1, 2);
        assertThat(predicate.test(event1)).isTrue();

        PrototypeEventInstance event2 = createIJEvent(2, 2);
        assertThat(predicate.test(event2)).isTrue();

        PrototypeEventInstance event3 = createIJEvent(2, 1);
        assertThat(predicate.test(event3)).isFalse();
    }
}
