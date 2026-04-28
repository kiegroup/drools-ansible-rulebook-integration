package org.drools.ansible.rulebook.integration.loadtests;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutcomeCheckTest {

    @Test
    void matchExpected_withMatches_passes() {
        assertThatCode(() -> OutcomeCheck.verify(1, ExpectedOutcome.MATCH, "24kb_1k_events.json"))
                .doesNotThrowAnyException();
    }

    @Test
    void matchExpected_withZeroMatches_throws() {
        assertThatThrownBy(() -> OutcomeCheck.verify(0, ExpectedOutcome.MATCH, "24kb_1k_events.json"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Expected at least one match but got 0")
                .hasMessageContaining("24kb_1k_events.json");
    }

    @Test
    void noMatchExpected_withZeroMatches_passes() {
        assertThatCode(() -> OutcomeCheck.verify(0, ExpectedOutcome.NO_MATCH, "retention_100_events.json"))
                .doesNotThrowAnyException();
    }

    @Test
    void noMatchExpected_withSomeMatches_throws() {
        assertThatThrownBy(() -> OutcomeCheck.verify(2, ExpectedOutcome.NO_MATCH, "24kb_1k_events_unmatch.json"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Expected no matches but got 2")
                .hasMessageContaining("24kb_1k_events_unmatch.json");
    }
}
