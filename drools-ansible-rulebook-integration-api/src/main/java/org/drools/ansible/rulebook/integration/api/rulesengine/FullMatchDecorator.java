package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kie.api.definition.rule.Rule;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;

public class FullMatchDecorator extends EmptyMatchDecorator {

    public FullMatchDecorator(Match delegate) {
        super(delegate);
    }

    @Override
    public List<? extends FactHandle> getFactHandles() {
        return delegate.getFactHandles();
    }

    @Override
    public List<Object> getObjects() {
        List<Object> objects = super.getObjects();
        objects.addAll( delegate.getObjects() );
        return objects;
    }

    @Override
    public List<String> getDeclarationIds() {
        List<String> ids = super.getDeclarationIds();
        ids.addAll( delegate.getDeclarationIds() );
        return ids;
    }

    @Override
    public Object getDeclarationValue(String declarationId) {
        Object object = super.getDeclarationValue(declarationId);
        if (object == null) {
            object = delegate.getDeclarationValue(declarationId);
        }
        return object;
    }
}