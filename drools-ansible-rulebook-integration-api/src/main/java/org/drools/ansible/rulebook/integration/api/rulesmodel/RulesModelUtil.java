package org.drools.ansible.rulebook.integration.api.rulesmodel;

import java.util.Map;

import org.drools.core.facttemplates.Fact;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.DEFAULT_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedEvent;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedFact;

public class RulesModelUtil {

    private RulesModelUtil() { }

    public static Fact mapToFact(Map<String, Object> factMap, boolean event) {
        Fact fact = event ? createMapBasedEvent( getPrototype(DEFAULT_PROTOTYPE_NAME) ) : createMapBasedFact( getPrototype(DEFAULT_PROTOTYPE_NAME) );
        populateFact(fact, factMap, "");
        return fact;
    }

    private static void populateFact(Fact fact, Map<?, ?> value, String fieldName) {
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            String key = fieldName + entry.getKey();
            fact.set(key, entry.getValue());
            if (entry.getValue() instanceof Map) {
                populateFact(fact, (Map<?, ?>) entry.getValue(), key + ".");
            }
        }
    }
}
