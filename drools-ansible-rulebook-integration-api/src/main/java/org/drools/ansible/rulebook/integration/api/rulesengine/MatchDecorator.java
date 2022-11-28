package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.kie.api.definition.rule.Rule;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;

public class MatchDecorator implements Match {
    private final Match delegate;
    private final Map<String, Object> boundObjects = new HashMap<>();

    public MatchDecorator(Match delegate) {
        this.delegate = delegate;
    }

    @Override
    public Rule getRule() {
        return delegate.getRule();
    }

    @Override
    public List<? extends FactHandle> getFactHandles() {
        return delegate.getFactHandles();
    }

    @Override
    public List<Object> getObjects() {
        List<Object> objects = new ArrayList<>();
        objects.addAll( delegate.getObjects() );
        objects.addAll( boundObjects.values() );
        return objects;
    }

    @Override
    public List<String> getDeclarationIds() {
        List<String> ids = new ArrayList<>();
        ids.addAll( delegate.getDeclarationIds() );
        ids.addAll( boundObjects.keySet() );
        return ids;
    }

    @Override
    public Object getDeclarationValue(String declarationId) {
        Object object = boundObjects.get(declarationId);
        if (object == null) {
            object = delegate.getDeclarationValue(declarationId);
        }
        return object;
    }

    public MatchDecorator withBoundObject(String bindingName, Object fact) {
        boundObjects.put(bindingName, fact);
        return this;
    }
}