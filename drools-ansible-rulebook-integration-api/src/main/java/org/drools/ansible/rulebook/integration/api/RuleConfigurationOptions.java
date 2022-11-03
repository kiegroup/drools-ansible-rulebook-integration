package org.drools.ansible.rulebook.integration.api;

public class RuleConfigurationOptions {
    private final RuleConfigurationOption[] options;

    public RuleConfigurationOptions(RuleConfigurationOption[] options) {
        this.options = options;
    }

    public boolean hasOption(RuleConfigurationOption option) {
        if (options == null) {
            return false;
        }
        for (RuleConfigurationOption op : options) {
            if (op == option) {
                return true;
            }
        }
        return false;
    }
}
