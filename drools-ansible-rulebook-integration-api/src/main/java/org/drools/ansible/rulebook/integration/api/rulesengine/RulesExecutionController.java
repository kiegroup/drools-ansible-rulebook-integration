package org.drools.ansible.rulebook.integration.api.rulesengine;

public class RulesExecutionController {

    public volatile boolean executeActions = true;

    public boolean executeActions() {
        return executeActions;
    }

    public void setExecuteActions(boolean executeActions) {
        this.executeActions = executeActions;
    }
}