package org.drools.ansible.rulebook.integration.api.rulesmodel;

import java.util.HashMap;
import java.util.Map;

import org.drools.model.Prototype;
import org.drools.model.PrototypeDSL;

public class PrototypeFactory {

    public static final String DEFAULT_PROTOTYPE_NAME = "DROOLS_PROTOTYPE";
    public static final String SYNTHETIC_PROTOTYPE_NAME = "DROOLS_SYNTHETIC_PROTOTYPE";

    private final Map<String, Prototype> prototypes = new HashMap<>();

    public Prototype getPrototype(String name) {
        return prototypes.computeIfAbsent(name, PrototypeDSL::prototype);
    }

    public Prototype getPrototype() {
        return getPrototype(DEFAULT_PROTOTYPE_NAME);
    }
}
