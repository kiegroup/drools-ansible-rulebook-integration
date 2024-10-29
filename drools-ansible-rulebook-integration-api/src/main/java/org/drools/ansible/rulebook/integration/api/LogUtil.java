package org.drools.ansible.rulebook.integration.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class LogUtil {

    private LogUtil() {
        // utility class
    }

    // convert java class to python class
    private static Map<Class<?>, String> convertMap = Map.of(
            Integer.class, "int",
            Boolean.class, "bool",
            String.class, "str",
            Double.class, "float"
    );

    public static String convertJavaClassToPythonClass(Class<?> javaClass) {
        if (convertMap.containsKey(javaClass)) {
            return convertMap.get(javaClass);
        }
        if (List.class.isAssignableFrom(javaClass)) {
            return "list";
        } else if (Map.class.isAssignableFrom(javaClass)) {
            return "dict";
        } else if (Set.class.isAssignableFrom(javaClass)) {
            return "set";
        }
        return javaClass.getSimpleName();
    }
}
