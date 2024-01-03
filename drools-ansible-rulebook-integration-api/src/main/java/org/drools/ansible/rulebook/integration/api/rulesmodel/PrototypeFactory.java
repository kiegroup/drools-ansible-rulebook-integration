package org.drools.ansible.rulebook.integration.api.rulesmodel;

import org.drools.model.prototype.Prototype;
import org.drools.model.prototype.PrototypeDSL;

import java.util.HashMap;
import java.util.Map;

public class PrototypeFactory {

    public static final String DEFAULT_PROTOTYPE_NAME = "DROOLS_PROTOTYPE";
    public static final String SYNTHETIC_PROTOTYPE_NAME = "DROOLS_SYNTHETIC_PROTOTYPE";

    private PrototypeFactory() { }

    private static final Map<String, Prototype> prototypes = new HashMap<>();

    public static Prototype getPrototype(String name) {
        return prototypes.computeIfAbsent(name, PrototypeDSL::prototype);
    }
}
