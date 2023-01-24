package org.drools.ansible.rulebook.integration.api.domain.actions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.model.Drools;

public class MapAction extends HashMap implements Action {

    private final List<Action> knownActions = new ArrayList<>();

    @Override
    public Object put(Object key, Object value) {
        switch(key.toString()) {
            case AssertFact.ACTION_NAME:
            case RetractFact.ACTION_NAME:
            case PostEvent.ACTION_NAME:
                String ruleset = (String)((Map) value).get("ruleset");
                Map<String, Object> fact = (Map<String, Object>)((Map) value).get("fact");
                if (ruleset != null && fact != null) {
                    FactAction factAction = createFactAction(key);
                    factAction.setRuleset(ruleset);
                    factAction.setFact(fact);
                    knownActions.add(factAction);
                }
                break;
            case RunPlaybook.ACTION_NAME:
                for (Map playbook : ((Collection<Map>) value)) {
                    String name = (String)playbook.get("name");
                    if (name != null) {
                        RunPlaybook runPlaybook = new RunPlaybook();
                        runPlaybook.setName(name);
                        knownActions.add(runPlaybook);
                    }
                }

        }
        return super.put(key, value);
    }

    private FactAction createFactAction(Object key) {
        if (key.toString().equals(RetractFact.ACTION_NAME)) {
            return new RetractFact();
        }
        if (key.toString().equals(PostEvent.ACTION_NAME)) {
            return new PostEvent();
        }
        return new AssertFact();
    }

    @Override
    public void execute(Drools drools) {
        knownActions.forEach( a -> a.execute(drools));
    }
}
