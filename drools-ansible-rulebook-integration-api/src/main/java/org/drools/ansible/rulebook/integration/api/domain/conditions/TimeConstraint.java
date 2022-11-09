package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.function.BiConsumer;

import org.drools.model.Drools;
import org.drools.model.PrototypeFact;
import org.drools.model.PrototypeVariable;
import org.drools.model.Rule;
import org.drools.model.view.ViewItem;

public interface TimeConstraint {

    ViewItem appendTimeConstraint(ViewItem pattern);

    default PrototypeVariable getTimeConstraintConsequenceVariable() {
        return null;
    }

    default BiConsumer<Drools, PrototypeFact> getTimeConstraintConsequence() {
        return (Drools drools, PrototypeFact fact) -> { };
    }

    default Rule getControlRule() {
        return null;
    }
}
