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

package org.drools.ansible.rulebook.integration.api.utils;

import java.util.LinkedHashMap;
import java.util.List;

/*
 * ToPatternTestBase also has several utility methods. If they are common to other tests, they should be moved here.
 */
public class TestUtils {

    private TestUtils() {
    }

    public static LinkedHashMap<Object, Object> createLhsRhsMap(LinkedHashMap lhs, LinkedHashMap rhs) {
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        map.put("lhs", lhs);
        map.put("rhs", rhs);
        return map;
    }

    public static LinkedHashMap<Object, Object> createSingleMap(Object key, Object value) {
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        map.put(key, value);
        return map;
    }

    public static LinkedHashMap<Object, Object> createEventField(String fieldName) {
        return createSingleMap("Event", fieldName);
    }

    public static LinkedHashMap<Object, Object> createRhsWithOperatorAndValue(String operatorType, String operatorValue, String valueType, Object valueValue) {
        LinkedHashMap<Object, Object> rhsValueMap = new LinkedHashMap<>();
        rhsValueMap.put("operator", createSingleMap(operatorType, operatorValue));
        rhsValueMap.put("value", createSingleMap(valueType, valueValue));
        return rhsValueMap;
    }

    public static org.drools.model.Rule getRuleByName(List<org.drools.model.Rule> rules, String ruleName) {
        return rules.stream().filter(r -> r.getName().equals(ruleName)).findFirst().get();
    }
}
