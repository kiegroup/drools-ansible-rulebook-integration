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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.conditions.MapCondition;
import org.drools.core.facttemplates.Event;
import org.drools.model.functions.Predicate1;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MapConditionToPatternAdditionTest extends ToPatternTestBase {

    @Test
    public void additionExpression() throws Exception {
        // Create MapCondition
        LinkedHashMap<Object, Object> lhsValueMap = createLhsForEventField("nested.i");
        LinkedHashMap<Object, Object> rhsValueMap = createAdditionExpression("Event", "nested.j", "Integer", 1);
        LinkedHashMap<Object, Object> equalsExpression = createEqualsExpression(lhsValueMap, rhsValueMap);
        LinkedHashMap<Object, Object> rootMap = createAllCondition(equalsExpression);
        MapCondition mapCondition = new MapCondition(rootMap);

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(mapCondition);

        Map<String, Object> nestedFactMap1 = new HashMap<>();
        nestedFactMap1.put("i", 1);
        nestedFactMap1.put("j", 2);
        Event event1 = createNestedEvent(nestedFactMap1);
        assertThat(predicate.test(event1)).isFalse();

        Map<String, Object> nestedFactMap2 = new HashMap<>();
        nestedFactMap2.put("i", 2);
        nestedFactMap2.put("j", 1);
        Event event2 = createNestedEvent(nestedFactMap2);
        assertThat(predicate.test(event2)).isTrue();
    }
}
