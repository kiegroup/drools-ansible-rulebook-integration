package org.drools.ansible.rulebook.integration.api;

public enum RuleConfigurationOption {
    EVENTS_PROCESSING,
    USE_PSEUDO_CLOCK,
    /**
     * When this option is set, there is no automatic advance of the internal pseudoclock.
     * This option is intended for use in testing
     * (avoid during debug session for the pseudoclock to diverge
     * because of the default automatic advancements)
     */
    FULLY_MANUAL_PSEUDOCLOCK,
    ASYNC_EVALUATION
}
