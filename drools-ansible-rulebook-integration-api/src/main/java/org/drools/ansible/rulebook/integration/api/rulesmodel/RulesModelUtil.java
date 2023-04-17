package org.drools.ansible.rulebook.integration.api.rulesmodel;

import org.drools.core.facttemplates.Fact;
import org.json.JSONObject;

import java.util.Map;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.DEFAULT_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedEvent;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedFact;

public class RulesModelUtil {

    public static final String ORIGINAL_MAP_FIELD = "$_ORIGINAL_$_MAP_$_FIELD_$";

    private RulesModelUtil() { }

    public static Fact mapToFact(Map<String, Object> factMap, boolean event) {
        Fact fact = event ? createMapBasedEvent( getPrototype(DEFAULT_PROTOTYPE_NAME) ) : createMapBasedFact( getPrototype(DEFAULT_PROTOTYPE_NAME) );
        populateFact(fact, factMap, "");
        fact.set(ORIGINAL_MAP_FIELD, factMap);
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

    public static Object factToMap(Object fact) {
        if (fact instanceof Fact) {
            return factToMap(((Fact) fact).asMap());
        }
        if (fact instanceof Map) {
            return factToMap(((Map) fact));
        }
        return fact;
    }

    private static Map<String, Object> factToMap(Map<String, Object> factMap) {
        Map<String, Object> map = (Map<String, Object>) factMap.get(ORIGINAL_MAP_FIELD);
        return map != null ? map : factMap;
    }

    public static Map<String, Object> asFactMap(String json) {
        return new JSONObject(json).toMap();
    }
}
