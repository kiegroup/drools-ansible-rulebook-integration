package org.drools.ansible.rulebook.integration.api;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class RuleConfigurationOptions {
    private final Set<RuleConfigurationOption> options;

    public RuleConfigurationOptions(RuleConfigurationOption... options) {
        this.options = new HashSet<>( Arrays.asList( options ) );
    }

    public boolean hasOption(RuleConfigurationOption option) {
        return options.contains(option);
    }

    public void addOption(RuleConfigurationOption option) {
        options.add(option);
    }
}
