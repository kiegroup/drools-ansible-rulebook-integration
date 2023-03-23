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
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.conditions.SimpleCondition;
import org.drools.core.facttemplates.Event;
import org.drools.model.functions.Predicate1;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleConditionToPatternTest extends ToPatternTestBase {

    @Test
    public void simpleConditionLogicalOperator() throws Exception {
        // Create SimpleCondition
        SimpleCondition simpleCondition = new SimpleCondition("sensu.data.i == 1");

        // toPattern and extract its predicate
        Predicate1.Impl predicate = toPatternAndGetFirstPredicate(simpleCondition);

        Event event1 = createSensuEvent(1);
        assertThat(predicate.test(event1)).isTrue();

        Event event2 = createSensuEvent(2);
        assertThat(predicate.test(event2)).isFalse();
    }

    // create an Event which has nested field "sensu.data.i"
    private Event createSensuEvent(Integer i) {
        Map<String, Object> iMap = new HashMap<>();
        iMap.put("i", i);
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("data", iMap);
        Map<String, Object> sensuMap = new HashMap<>();
        sensuMap.put("sensu", dataMap);
        return createEvent(sensuMap);
    }
}
