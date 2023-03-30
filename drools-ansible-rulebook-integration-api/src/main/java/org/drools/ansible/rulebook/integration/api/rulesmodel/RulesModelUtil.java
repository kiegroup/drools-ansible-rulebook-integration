package org.drools.ansible.rulebook.integration.api.rulesmodel;

import java.util.HashMap;
import java.util.Map;

import org.drools.core.facttemplates.Fact;
import org.json.JSONObject;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.DEFAULT_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedEvent;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedFact;

public class RulesModelUtil {

    private static final String ORIGINAL_MAP_FIELD = "$_ORIGINAL_$_MAP_$_FIELD_$";

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

    public static Map<String, Object> factToMap(Fact fact) {
        Map<String, Object> map = (Map<String, Object>) fact.get(ORIGINAL_MAP_FIELD);
        return map != null ? map : fact.asMap();
    }

    public static Map<String, Object> addOriginalMap(Map<String, Object> factMap) {
        factMap.put(ORIGINAL_MAP_FIELD, new HashMap<>(factMap));
        return factMap;
    }

    public static Map<String, Object> removeOriginalMap(Map<String, Object> factMap) {
        factMap.remove(ORIGINAL_MAP_FIELD);
        return factMap;
    }

    public static Map<String, Object> asFactMap(String json) {
        return new JSONObject(json).toMap();
    }
}
