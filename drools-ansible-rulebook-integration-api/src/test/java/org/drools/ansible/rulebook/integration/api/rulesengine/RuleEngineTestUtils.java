package org.drools.ansible.rulebook.integration.api.rulesengine;

public class RuleEngineTestUtils {

    private RuleEngineTestUtils() {
        // Utility class
    }

    public static void enableEventStructureSuggestion() {
        RulesSetEventStructure.EVENT_STRUCTURE_SUGGESTION_ENABLED = true;
    }

    public static void disableEventStructureSuggestion() {
        RulesSetEventStructure.EVENT_STRUCTURE_SUGGESTION_ENABLED = false;
    }
}
