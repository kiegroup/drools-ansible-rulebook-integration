package org.drools.ansible.rulebook.integration.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class LogUtil {

    private LogUtil() {
        // utility class
    }

    // convert java class to python class
    private static Map<Class<?>, String> convertMap = Map.of(
            Integer.class, "int",
            Boolean.class, "bool",
            String.class, "str",
            Double.class, "float",
            List.class, "list",
            ArrayList.class, "list",
            Map.class, "dict",
            LinkedHashMap.class, "dict",
            HashMap.class, "dict"
    );

    public static String convertJavaClassToPythonClass(Class<?> javaClass) {
        if (convertMap.containsKey(javaClass)) {
            return convertMap.get(javaClass);
        }
        if (List.class.isAssignableFrom(javaClass)) {
            return "list";
        } else if (Map.class.isAssignableFrom(javaClass)) {
            return "dict";
        }
        return javaClass.getSimpleName();
    }
}
