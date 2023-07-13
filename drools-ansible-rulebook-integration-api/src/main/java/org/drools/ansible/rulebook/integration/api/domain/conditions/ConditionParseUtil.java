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

    /*
     * implements `selectattr()` per jinja2/Ansible behaviour
     * 
     * ie: no braket accessor support, but it actually works by splitting on `.` "brutally"
     * 
     * A. `selectatrr()` is "a bit underspecified" in the Ansible playbook manual; that is, the behaviour is not described beyond referencing it to Jinja.
     * ref https://docs.ansible.com/ansible/latest/playbook_guide/complex_data_manipulation.html
     * B. in Jinja manual, it is "a bit underspecified" there in the ultimate sense nowhere in the docs I seem to find how the 1st string argument is supposed to behave (just a key? or a "dot accessor expression"?).
     * ref https://jinja.palletsprojects.com/en/3.1.x/templates/#jinja-filters.selectattr
     * C. looking at jinja source code I need to perform at least 3 hops before finding any mention if the 1st argument is directly a key or a "dot accessor expression":
     * https://github.com/pallets/jinja/blob/7b48764688dce68e99341bec59b1eedf0c5c3ba5/src/jinja2/filters.py#L1589
     * https://github.com/pallets/jinja/blob/7b48764688dce68e99341bec59b1eedf0c5c3ba5/src/jinja2/filters.py#L1757
     * https://github.com/pallets/jinja/blob/7b48764688dce68e99341bec59b1eedf0c5c3ba5/src/jinja2/filters.py#L1723
     * https://github.com/pallets/jinja/blob/7b48764688dce68e99341bec59b1eedf0c5c3ba5/src/jinja2/filters.py#L62-L63
     * and appears to be actually manipulated by splitting on the `.`.
     * 
     * See also: https://github.com/ansible/ansible-rulebook/pull/539#discussion_r1243345097
     */
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

    public static String mapToStringValue(Object rhsValue) {
        Map<?,?> rhsMap = (Map<?,?>)rhsValue;
        if (rhsMap.size() != 1) {
            throw new UnsupportedOperationException("The map " + rhsMap + " must have exactly 1 entry");
        }
        return rhsMap.entrySet().iterator().next().getValue().toString().trim();
    }
}
