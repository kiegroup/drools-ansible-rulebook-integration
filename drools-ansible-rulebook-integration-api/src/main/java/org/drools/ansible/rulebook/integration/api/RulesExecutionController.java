package org.drools.ansible.rulebook.integration.api;

public class RulesExecutionController {

    public boolean executeActions = true;

    public boolean executeActions() {
        return executeActions;
    }

    public void setExecuteActions(boolean executeActions) {
        this.executeActions = executeActions;
    }
}