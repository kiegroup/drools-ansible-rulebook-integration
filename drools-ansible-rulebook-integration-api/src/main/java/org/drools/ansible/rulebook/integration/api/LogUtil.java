package org.drools.ansible.rulebook.integration.api;

import java.util.Map;

public class LogUtil {

    private LogUtil() {
        // utility class
    }

    // convert java class to python class
    private static Map<Class<?>, String> convertMap = Map.of(
            java.lang.Integer.class, "int",
            java.lang.Boolean.class, "bool",
            java.lang.String.class, "str",
            java.lang.Double.class, "float",
            java.util.List.class, "list",
            java.util.ArrayList.class, "list",
            java.util.Map.class, "dict",
            java.util.LinkedHashMap.class, "dict",
            java.util.HashMap.class, "dict"
    );

    public static String convertJavaClassToPythonClass(Class<?> javaClass) {
        return convertMap.getOrDefault(javaClass, javaClass.getSimpleName());
    }
}
