package org.drools.ansible.rulebook.integration.api.rulesmodel;

import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.kie.api.prototype.PrototypeFactInstance;

import java.util.HashMap;
import java.util.Map;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.DEFAULT_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototypeEvent;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototypeFact;

public class RulesModelUtil {
    public static final String META_FIELD = "meta";
    public static final String RULE_ENGINE_META_FIELD = "rule_engine";

    private RulesModelUtil() { }

    public static PrototypeFactInstance mapToFact(Map<String, Object> factMap, boolean event) {
        PrototypeFactInstance fact = event ? getPrototypeEvent(DEFAULT_PROTOTYPE_NAME).newInstance() : getPrototypeFact(DEFAULT_PROTOTYPE_NAME).newInstance();
        factMap.forEach(fact::put);
        return fact;
    }

    public static Object factToMap(Object fact) {
        if (fact instanceof PrototypeFactInstance) {
            return ((PrototypeFactInstance) fact).asMap();
        }
        return fact;
    }

    public static Map<String, Object> asFactMap(String json) {
        return JsonMapper.readValueAsMapOfStringAndObject(json);
    }

    public static PrototypeFactInstance writeMetaDataOnEvent(PrototypeFactInstance event, Map ruleEngineMeta) {
        Map map = event.asMap();
        Map meta = (Map) map.computeIfAbsent(META_FIELD, x -> new HashMap<>());
        meta.put(RULE_ENGINE_META_FIELD, ruleEngineMeta);
        return event;
    }
}
