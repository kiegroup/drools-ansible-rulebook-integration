package org.drools.ansible.rulebook.integration.visualization.parser;

import org.drools.ansible.rulebook.integration.api.domain.actions.MapAction;
import org.drools.base.prototype.PrototypeObjectType;
import org.drools.impact.analysis.model.right.DeleteSpecificFactAction;
import org.drools.impact.analysis.model.right.InsertAction;
import org.drools.impact.analysis.model.right.InsertedProperty;
import org.drools.impact.analysis.model.right.RightHandSide;
import org.drools.impact.analysis.model.right.SpecificProperty;

import java.util.List;
import java.util.Map;

public class RhsParser {

    private RhsParser() {
        // intentionally private
    }

    public static void parse(List<MapAction> mapActions, RightHandSide rhs) {
        for (MapAction mapAction : mapActions) {
            Map map = (Map) mapAction.get("Action");
            String action = (String) map.get("action");
            Map actionArgs = (Map) map.get("action_args");
            switch (action) {
                case "post_event":
                    addInsertAction((Map<String, String>) actionArgs.get("event"), rhs);
                    break;
                case "set_fact":
                    addInsertAction((Map<String, String>) actionArgs.get("fact"), rhs);
                    break;
                case "retract_fact":
                    addDeleteAction((Map<String, String>) actionArgs.get("fact"), rhs);
                    break;
                default:
                    // ignore any other actions
            }
        }
    }

    private static void addInsertAction(Map<String, String> propertyMap, RightHandSide rhs) {
        InsertAction action = new InsertAction(PrototypeObjectType.class);
        propertyMap.entrySet().forEach(entry -> {
            InsertedProperty insertedProperty = new InsertedProperty(entry.getKey(), entry.getValue());
            action.addInsertedProperty(insertedProperty);
        });
        rhs.addAction(action);
    }

    private static void addDeleteAction(Map<String, String> propertyMap, RightHandSide rhs) {
        DeleteSpecificFactAction action = new DeleteSpecificFactAction(PrototypeObjectType.class);
        propertyMap.entrySet().forEach(entry -> {
            SpecificProperty specificProperty = new SpecificProperty(entry.getKey(), entry.getValue());
            action.addSpecificProperty(specificProperty);
        });
        rhs.addAction(action);
    }
}
