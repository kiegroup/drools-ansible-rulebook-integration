package org.drools.ansible.rulebook.integration.loadtests;

public final class OutcomeCheck {

    private OutcomeCheck() {}

    public static void verify(int matchCount, ExpectedOutcome expected, String eventsJson) {
        if (expected == ExpectedOutcome.MATCH && matchCount == 0) {
            throw new RuntimeException(
                    "Expected at least one match but got 0 (events: " + eventsJson + ")");
        }
        if (expected == ExpectedOutcome.NO_MATCH && matchCount > 0) {
            throw new RuntimeException(
                    "Expected no matches but got " + matchCount
                            + " (events: " + eventsJson + ")");
        }
    }
}
