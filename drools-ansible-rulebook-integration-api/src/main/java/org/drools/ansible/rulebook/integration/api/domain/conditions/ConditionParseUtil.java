package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.drools.model.Prototype;
import org.json.JSONObject;

public class ConditionParseUtil {

    private static final Set<String> KNOWN_TYPES = Set.of("Integer", "Float", "String", "Boolean", "NullType");

    public static boolean isKnownType(String type) {
        return KNOWN_TYPES.contains(type);
    }

    public static boolean isRegexOperator(String operator) {
        return operator.equals("match") || operator.equals("search") || operator.equals("regex");
    }

    public static Object toJsonValue(Object object) {
        if (object instanceof Map) {
            return toJsonValue( ((Map<?,?>)object).entrySet().iterator().next() );
        }
        if (object instanceof Collection) {
            return ((Collection) object).stream().map(ConditionParseUtil::toJsonValue).collect(Collectors.toList());
        }
        throw new UnsupportedOperationException();
    }

    public static Object toJsonValue(Map.Entry entry) {
        return toJsonValue((String) entry.getKey(), entry.getValue());
    }

    public static Object toJsonValue(String type, Object value) {
        if (value == null) {
            return null;
        }
        if (type.equals("String")) {
            return value.toString();
        }
        return JSONObject.stringToValue(value.toString());
    }

    public static Object extractMapAttribute(Map map, String attr) {
        if (map == null) {
            return null;
        }
        int dotPos = attr.indexOf('.');
        if (dotPos < 0) {
            return map.containsKey(attr) ? map.get(attr) : Prototype.UNDEFINED_VALUE;
        }
        if (!map.containsKey(attr.substring(0, dotPos))) {
            return Prototype.UNDEFINED_VALUE;
        }
        return extractMapAttribute( (Map) map.get(attr.substring(0, dotPos)), attr.substring(dotPos+1) );
    }

    public static boolean isEventOrFact(String key) {
        return key.equalsIgnoreCase("fact") || key.equalsIgnoreCase("facts") || key.equalsIgnoreCase("event") || key.equalsIgnoreCase("events");
    }
}
