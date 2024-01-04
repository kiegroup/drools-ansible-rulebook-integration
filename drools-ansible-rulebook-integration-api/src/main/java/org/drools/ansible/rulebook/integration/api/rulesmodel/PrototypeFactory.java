package org.drools.ansible.rulebook.integration.api.rulesmodel;

import org.drools.model.prototype.PrototypeDSL;
import org.kie.api.prototype.PrototypeEvent;
import org.kie.api.prototype.PrototypeFact;

import java.util.HashMap;
import java.util.Map;

public class PrototypeFactory {

    public static final String DEFAULT_PROTOTYPE_NAME = "DROOLS_PROTOTYPE";
    public static final String SYNTHETIC_PROTOTYPE_NAME = "DROOLS_SYNTHETIC_PROTOTYPE";

    private PrototypeFactory() { }

    private static final Map<String, PrototypeFact> prototypeFacts = new HashMap<>();
    private static final Map<String, PrototypeEvent> prototypeEvents = new HashMap<>();

    public static PrototypeFact getPrototypeFact(String name) {
        return prototypeFacts.computeIfAbsent(name, PrototypeDSL::prototypeFact);
    }

    public static PrototypeEvent getPrototypeEvent(String name) {
        return prototypeEvents.computeIfAbsent(name, PrototypeDSL::prototypeEvent);
    }
}
