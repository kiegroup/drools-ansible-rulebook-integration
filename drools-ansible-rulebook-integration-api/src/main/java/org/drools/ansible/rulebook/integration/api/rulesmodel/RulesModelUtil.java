package org.drools.ansible.rulebook.integration.api.rulesmodel;

import org.drools.base.facttemplates.Fact;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.DEFAULT_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototype;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedEvent;
import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedFact;

public class RulesModelUtil {
    public static final String META_FIELD = "meta";
    public static final String RULE_ENGINE_META_FIELD = "rule_engine";

    private RulesModelUtil() { }

    public static Fact mapToFact(Map<String, Object> factMap, boolean event) {
        Fact fact = event ? createMapBasedEvent( getPrototype(DEFAULT_PROTOTYPE_NAME), factMap ) : createMapBasedFact( getPrototype(DEFAULT_PROTOTYPE_NAME), factMap );
        return fact;
    }

    public static Object factToMap(Object fact) {
        if (fact instanceof Fact) {
            return ((Fact) fact).asMap();
        }
        return fact;
    }

    public static Map<String, Object> asFactMap(String json) {
        return new JSONObject(json).toMap();
    }

    public static Fact writeMetaDataOnEvent(Fact event, Map ruleEngineMeta) {
        Map map = (Map) event.asMap();
        Map meta = (Map) map.computeIfAbsent(META_FIELD, x -> new HashMap<>());
        meta.put(RULE_ENGINE_META_FIELD, ruleEngineMeta);
        return event;
    }
}
