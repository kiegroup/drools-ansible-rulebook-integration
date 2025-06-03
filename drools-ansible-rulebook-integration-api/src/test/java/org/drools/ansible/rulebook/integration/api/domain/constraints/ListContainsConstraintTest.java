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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ListContainsConstraintTest {

    @Test
    void integerListAgainstBigDecimal() {
        List<Integer> list = Arrays.asList(1, 2, 3);
        boolean result = ListContainsConstraint.INSTANCE.asPredicate().test(list, new BigDecimal("1.0"));
        assertThat(result).isTrue();
    }

    @Test
    void doubleListAgainstBigDecimal() {
        List<Double> list = Arrays.asList(1.0, 2.0, 3.0);
        boolean result = ListContainsConstraint.INSTANCE.asPredicate().test(list, new BigDecimal("1.0"));
        assertThat(result).isTrue();
    }

    @Test
    void bigDecimalListAgainstInteger() {
        List<BigDecimal> list = Arrays.asList(new BigDecimal("1.0"), new BigDecimal("2.0"), new BigDecimal("3.0"));
        boolean result = ListContainsConstraint.INSTANCE.asPredicate().test(list, 1);
        assertThat(result).isTrue();
    }

    @Test
    void bigDecimalListWithDifferentScaleAgainstBigDecimal() {
        List<BigDecimal> list = Arrays.asList(new BigDecimal("1"), new BigDecimal("1.00"));
        boolean result = ListContainsConstraint.INSTANCE.asPredicate().test(list, new BigDecimal("1.0"));
        assertThat(result).isTrue();
    }

    @Test
    void bigDecimalListWithDifferentScaleAgainstInteger() {
        List<BigDecimal> list = Arrays.asList(new BigDecimal("1"), new BigDecimal("1.00"));
        boolean result = ListContainsConstraint.INSTANCE.asPredicate().test(list, 1);
        assertThat(result).isTrue();
    }
}
