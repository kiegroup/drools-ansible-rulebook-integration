package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.json.JSONObject;

public class ConditionParseUtil {
    public static boolean isKnownType(String type) {
        return type.equals("Integer") || type.equals("Float") || type.equals("String") || type.equals("Boolean");
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
        if (type.equals("String")) {
            return value.toString();
        }
        return JSONObject.stringToValue(value.toString());
    }

    public static String toRegexPattern(String value, String operator) {
        if (operator.equals("match")) {
            return value + ".*";
        } else if (operator.equals("search") || operator.equals("regex")) {
            return ".*" + value + ".*";
        }
        return null;
    }

    public static Object extractMapAttribute(Map map, String attr) {
        if (map == null) {
            return null;
        }
        int dotPos = attr.indexOf('.');
        return dotPos < 0 ?
                map.get(attr) :
                extractMapAttribute( (Map) map.get(attr.substring(0, dotPos)), attr.substring(dotPos+1) );
    }
}
