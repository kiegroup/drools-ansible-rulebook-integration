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
import org.junit.jupiter.api.Test;
import org.kie.api.prototype.PrototypeEventInstance;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.createEventField;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.createRhsWithOperatorAndValue;

public class MapConditionToPatternSelectTest extends ToPatternTestBase {

    @Test
    void selectExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("levels");
        LinkedHashMap<Object, Object> rhsValueMap = createRhsWithOperatorAndValue("String", ">", "Integer", 25);
        LinkedHashMap<Object, Object> selectExpression = createSelectExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(selectExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        PrototypeEventInstance event1 = createEvent("levels", 25);
        assertThat(predicate.test(event1)).isFalse();

        PrototypeEventInstance event2 = createEvent("levels", 26);
        assertThat(predicate.test(event2)).isTrue();

        PrototypeEventInstance event3 = createEvent("levels", 10, 25, 26);
        assertThat(predicate.test(event3)).isTrue();
    }

    @Test
    void selectNotExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("levels");
        LinkedHashMap<Object, Object> rhsValueMap = createRhsWithOperatorAndValue("String", ">", "Integer", 25);
        LinkedHashMap<Object, Object> selectNotExpression = createSelectNotExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(selectNotExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        PrototypeEventInstance event1 = createEvent("levels", 25);
        assertThat(predicate.test(event1)).isTrue();

        PrototypeEventInstance event2 = createEvent("levels", 26, 30);
        assertThat(predicate.test(event2)).isFalse();

        PrototypeEventInstance event3 = createEvent("levels", 10, 25, 26);
        assertThat(predicate.test(event3)).isTrue();
    }

    @Test
    void selectExpressionWithRegex() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("addresses");
        LinkedHashMap<Object, Object> rhsValueMap = createRhsWithOperatorAndValue("String", "regex", "String", "Main St");
        LinkedHashMap<Object, Object> selectExpression = createSelectExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(selectExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        PrototypeEventInstance event1 = createEvent("addresses", "123 Main St, Bedrock, MI", "545 Spring St, Cresskill, NJ", "435 Wall Street, New York, NY");
        assertThat(predicate.test(event1)).isTrue();

        PrototypeEventInstance event2 = createEvent("addresses", "545 Spring St, Cresskill, NJ", "435 Wall Street, New York, NY");
        assertThat(predicate.test(event2)).isFalse();
    }

    @Test
    void selectExpressionOnField() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createEventField("my_list1");
        LinkedHashMap<Object, Object> rhsValueMap = createRhsWithOperatorAndValue("String", "==", "Event", "my_int1");
        LinkedHashMap<Object, Object> selectExpression = createSelectExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(selectExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        Map<String, Object> factMap1 = new HashMap<>();
        factMap1.put("my_int1", 3);
        factMap1.put("my_list1", Arrays.asList(1, 3, 7));
        PrototypeEventInstance event1 = createEvent(factMap1);
        assertThat(predicate.test(event1)).isTrue();

        Map<String, Object> factMap2 = new HashMap<>();
        factMap2.put("my_int1", 4);
        factMap2.put("my_list1", Arrays.asList(1, 3, 7));
        PrototypeEventInstance event2 = createEvent(factMap2);
        assertThat(predicate.test(event2)).isFalse();
    }
}
