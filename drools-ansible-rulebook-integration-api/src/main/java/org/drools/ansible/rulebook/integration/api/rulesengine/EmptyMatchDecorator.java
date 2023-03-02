package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kie.api.definition.rule.Rule;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;

public class EmptyMatchDecorator implements Match {
    protected final Match delegate;
    private final Map<String, Object> boundObjects = new HashMap<>();

    public EmptyMatchDecorator(Match delegate) {
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

// TODO - required since drools 8.36.0.Final
//    @Override
//    public int getSalience() {
//        return delegate.getSalience();
//    }

    @Override
    public int getSalience() {
        return delegate.getSalience();
    }

    @Override
    public List<Object> getObjects() {
        List<Object> objects = new ArrayList<>();
        objects.addAll( boundObjects.values() );
        return objects;
    }

    @Override
    public List<String> getDeclarationIds() {
        List<String> ids = new ArrayList<>();
        ids.addAll( boundObjects.keySet() );
        return ids;
    }

    @Override
    public Object getDeclarationValue(String declarationId) {
        return boundObjects.get(declarationId);
    }

    public EmptyMatchDecorator withBoundObject(String bindingName, Object fact) {
        boundObjects.put(bindingName, fact);
        return this;
    }
}