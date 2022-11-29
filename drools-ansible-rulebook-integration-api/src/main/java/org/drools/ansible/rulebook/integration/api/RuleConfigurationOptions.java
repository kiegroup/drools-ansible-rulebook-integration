package org.drools.ansible.rulebook.integration.api;

import java.util.HashSet;
import java.util.Set;

public class RuleConfigurationOptions {
    private final Set<RuleConfigurationOption> options = new HashSet<>();

    public boolean hasOption(RuleConfigurationOption option) {
        return options.contains(option);
    }

    public void addOptions(RuleConfigurationOption... options) {
        for (RuleConfigurationOption option : options) {
            addSingleOption(option);
        }
    }

    public void addOptions(Iterable<RuleConfigurationOption> options) {
        options.forEach(this::addSingleOption);
    }

    private void addSingleOption(RuleConfigurationOption option) {
        this.options.add(option);
    }

    public Set<RuleConfigurationOption> getOptions() {
        return options;
    }
}
