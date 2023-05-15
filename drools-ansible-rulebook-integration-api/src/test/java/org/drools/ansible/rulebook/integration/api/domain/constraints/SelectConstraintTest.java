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

package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BiPredicate;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.PrototypeExpression;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.createEventField;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.createLhsRhsMap;
import static org.drools.ansible.rulebook.integration.api.utils.TestUtils.createRhsWithOperatorAndValue;

public class SelectConstraintTest {

    @Test
    public void createParsedConditionWithSelectExpression() {
        LinkedHashMap<Object, Object> map = createLhsRhsMap(
                createEventField("levels"),
                createRhsWithOperatorAndValue("String", ">", "Integer", 25)
        );

        ParsedCondition parsedCondition = SelectConstraint.INSTANCE.createParsedCondition(new RuleGenerationContext(), SelectConstraint.EXPRESSION_NAME, map);

        assertThat(parsedCondition.getLeft()).isInstanceOf(PrototypeExpression.PrototypeFieldValue.class);
        assertThat(((PrototypeExpression.PrototypeFieldValue)parsedCondition.getLeft()).getFieldName()).isEqualTo("levels");

        assertThat(parsedCondition.getRight()).isInstanceOf(PrototypeExpression.FixedValue.class); // rightValue is not important. "25" is already incorporated in the operator

        BiPredicate<Object, Object> predicate = parsedCondition.getOperator().asPredicate();

        List<Integer> list1 = Arrays.asList(10, 20, 25);
        assertThat(predicate.test(list1, null)).isFalse();

        List<Integer> list2 = Arrays.asList(10, 20, 26);
        assertThat(predicate.test(list2, null)).as("If at least least one element(26) meets the condition, the result should be true").isTrue();
    }

    @Test
    public void createParsedConditionWithSelectNotExpression() {
        LinkedHashMap<Object, Object> map = createLhsRhsMap(
                createEventField("levels"),
                createRhsWithOperatorAndValue("String", ">", "Integer", 25)
        );

        ParsedCondition parsedCondition = SelectConstraint.INSTANCE.createParsedCondition(new RuleGenerationContext(), SelectConstraint.NEGATED_EXPRESSION_NAME, map);

        assertThat(parsedCondition.getLeft()).isInstanceOf(PrototypeExpression.PrototypeFieldValue.class);
        assertThat(((PrototypeExpression.PrototypeFieldValue)parsedCondition.getLeft()).getFieldName()).isEqualTo("levels");

        assertThat(parsedCondition.getRight()).isInstanceOf(PrototypeExpression.FixedValue.class); // rightValue is not important. "25" is already incorporated in the operator

        BiPredicate<Object, Object> predicate = parsedCondition.getOperator().asPredicate();

        List<Integer> list1 = Arrays.asList(25, 30, 40);
        assertThat(predicate.test(list1, null)).as("If at least one element(25) meets the condition negatively, the result should be true").isTrue();

        List<Integer> list2 = Arrays.asList(26, 30, 40);
        assertThat(predicate.test(list2, null)).isFalse();
    }
}
