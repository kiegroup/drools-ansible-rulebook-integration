package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.drools.ansible.rulebook.integration.api.domain.Rule;
import org.drools.ansible.rulebook.integration.api.domain.RuleContainer;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.domain.conditions.MapCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toSet;

/**
 * This class holds the event paths from the rules set.
 * Then, when the first event comes, validate the event paths comparing with incoming event structure.
 * If the event path doesn't match with the incoming event structure, suggest the possible typo or missing node.
 */
public class RulesSetEventStructure {

    private static final Logger LOG = LoggerFactory.getLogger(RulesSetEventStructure.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // key: event path split into nodes
    // value: list of rule names that use the event path
    private Map<List<String>, List<String>> eventPathMap = new HashMap<>();

    private boolean validated = false;

    private String ruleSetName;

    public RulesSetEventStructure(RulesSet rulesSet) {
        analyzeRulesSet(rulesSet);

        if (LOG.isDebugEnabled()) {
            LOG.debug("eventPathMap : {}", eventPathMap);
        }
    }

    private void analyzeRulesSet(RulesSet rulesSet) {

        this.ruleSetName = rulesSet.getName();

        for (RuleContainer ruleContainer : rulesSet.getRules()) {
            Rule rule = ruleContainer.getRule();
            if (rule.getCondition() instanceof MapCondition mapCondition) {
                traverseMap(mapCondition.getMap(), rule.getName());
            }
        }
    }

    private void traverseMap(Map<?, ?> map, String ruleName) {
        map.forEach((key, value) -> {
            if ((key.equals("Event") || key.equals("Fact")) && value instanceof String stringValue) {
                List<String> eventPathNodeList = splitEventPath(stringValue);
                eventPathMap.compute(eventPathNodeList, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(ruleName);
                    return v;
                });
                return;
            }

            if (value instanceof Map<?, ?> mapValue) {
                traverseMap(mapValue, ruleName);
            } else if (value instanceof List<?> listValue) {
                traverseList(listValue, ruleName);
            }
        });
    }

    private void traverseList(List<?> list, String ruleName) {
        list.forEach(item -> {
            if (item instanceof Map<?, ?> mapItem) {
                traverseMap(mapItem, ruleName);
            } else if (item instanceof List<?> listItem) {
                traverseList(listItem, ruleName);
            }
        });
    }

    /**
     * normalize the event path to this format.
     * person["name"] -> person.name
     * person[0] -> person[]
     * TODO: keeping the index person[0] might be better for the future validation.
     */
    private List<String> splitEventPath(String eventPath) {
        String[] nodes = eventPath.split("\\.");
        List<String> pathNodeList = new ArrayList<>();
        for (String node : nodes) {
            // handle bracket notation
            int leftBracket = node.indexOf('[');
            if (leftBracket > 0) {
                int rightBracket = node.indexOf(']');
                String body = node.substring(0, leftBracket);
                String key = node.substring(leftBracket + 1, rightBracket);
                if (isQuotedString(key)) {
                    // String key means it's a map access. Equivalent to property
                    key = key.substring(1, key.length() - 1);
                    pathNodeList.add(body);
                    pathNodeList.add(key);
                } else if (isInteger(key)) {
                    // Integer key means it's a list.
                    pathNodeList.add(body + "[]");
                } else {
                    LOG.warn("Unknown key type: {} in the node {}", key, node);
                    // return the path before the node
                    return pathNodeList;
                }

                // TODO: handle nested bracket pairs. e.g. event.asd["x"][1][2]
                if (rightBracket < node.length() - 1) {
                    LOG.info("Multiple bracket pairs. {}", node);
                    // return the path before the unparsed bracket
                    return pathNodeList;
                }
            } else {
                pathNodeList.add(node);
            }
        }
        return pathNodeList;
    }

    private static boolean isQuotedString(String key) {
        return key.length() >= 2 && (
                (key.charAt(0) == '"' && key.charAt(key.length() - 1) == '"')
                        ||
                        (key.charAt(0) == '\'' && key.charAt(key.length() - 1) == '\'')
        );
    }

    private static boolean isInteger(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        int i = 0;
        if (str.charAt(0) == '-') {
            if (str.length() == 1) {
                return false;
            }
            i = 1;
        }
        for (; i < str.length(); i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public boolean isValidated() {
        return validated;
    }

    /**
     * Validate the event path in RuleSets using the incoming event structure.
     * Assume that the incoming event has the correct structure.
     */
    public void validate(String incomingJson) {
        validated = true;

        JsonNode jsonNode;
        try {
            // incoming event structure
            jsonNode = OBJECT_MAPPER.readTree(incomingJson);
        } catch (JsonProcessingException e) {
            LOG.debug("Failed to parse the incoming event structure." +
                              " You may ignore this error because this feature doesn't affect the main task", e);
            return;
        }

        for (Map.Entry<List<String>, List<String>> eventPathEntry : eventPathMap.entrySet()) {
            validateEventPathWithEventStructure(eventPathEntry, jsonNode);
        }
    }

    private void validateEventPathWithEventStructure(Map.Entry<List<String>, List<String>> eventPathEntry, JsonNode rootJsonNode) {
        List<String> eventPathNodeList = eventPathEntry.getKey();
        List<String> ruleNames = eventPathEntry.getValue();
        JsonNode currentJsonNode = rootJsonNode;
        Set<String> nextKeyNames = nodeEntrySetToNodeNameKeySet(currentJsonNode.properties());

        for (int level = 0; level < eventPathNodeList.size(); level++) {
            String currentPathNode = eventPathNodeList.get(level);
            if (nextKeyNames.contains(currentPathNode)) {
                // move to the next level
                if (currentJsonNode.isObject()) {
                    currentJsonNode = currentJsonNode.get(trimBracket(currentPathNode));
                } else if (currentJsonNode.isArray()) {
                    // TODO: consider array nesting. Currently, assuming only one level of array.
                    currentJsonNode = currentJsonNode.get(0); // TODO: wildly choose the first element. Need to search an appropriate element.
                    currentJsonNode = currentJsonNode.get(trimBracket(currentPathNode)); // TODO: assuming the next node is an object node. Consider array node nesting.
                }

                // set up nextKeyNames
                if (currentJsonNode.isObject()) {
                    nextKeyNames = nodeEntrySetToNodeNameKeySet(currentJsonNode.properties());
                } else if (currentJsonNode.isArray()) {
                    // TODO: consider array nesting. Currently, assuming only one level of array.
                    nextKeyNames = iteratorToSet(currentJsonNode.elements()).stream().flatMap(node -> nodeEntrySetToNodeNameKeySet(node.properties()).stream()).collect(toSet());
                } else {
                    // reached to a leaf node
                    break;
                }
            } else {
                Optional<String> candidateForTypo = suggestTypo(currentPathNode, nextKeyNames);
                if (candidateForTypo.isPresent()) {
                    LOG.warn("'{}' in the condition '{}' in rule set '{}' rule {} does not meet with the incoming event name {}. Did you mean '{}'?",
                             currentPathNode, concatNodeList(eventPathNodeList), ruleSetName, ruleNames, nextKeyNames, candidateForTypo.get());
                }

                Optional<String> candidateForMissingNode = suggestMissingNode(currentPathNode, currentJsonNode);
                if (candidateForMissingNode.isPresent()) {
                    LOG.warn("'{}' in the condition '{}' in rule set '{}' rule {} does not meet with the incoming event name {}. Did you forget to include '{}'?",
                             currentPathNode, concatNodeList(eventPathNodeList), ruleSetName, ruleNames, nextKeyNames, candidateForMissingNode.get());
                }
                break;
            }
        }
    }

    private String trimBracket(String pathNode) {
        int leftBracket = pathNode.indexOf('[');
        if (leftBracket > 0) {
            return pathNode.substring(0, leftBracket);
        } else {
            return pathNode;
        }
    }

    /*
     * Convert the entry set to a set of node names. If the node is an array node, add "[]" to the node name.
     */
    private static Set<String> nodeEntrySetToNodeNameKeySet(Set<Map.Entry<String, JsonNode>> entrySet) {
        return entrySet.stream().map(entry -> {
            if (entry.getValue().isArray()) {
                return entry.getKey() + "[]";
            } else {
                return entry.getKey();
            }
        }).collect(toSet());
    }

    public static Set<JsonNode> iteratorToSet(Iterator<JsonNode> iterator) {
        Set<JsonNode> set = new HashSet<>();
        iterator.forEachRemaining(set::add);
        return set;
    }

    /*
     * Look ahead to next-next nodes. If matched, assume that the next node is missing.
     */
    private Optional<String> suggestMissingNode(String input, JsonNode currentJsonNode) {
        // array node doesn't have properties, so eventually return empty
        Set<Map.Entry<String, JsonNode>> nextEntries = currentJsonNode.properties();

        for (Map.Entry<String, JsonNode> nextEntry : nextEntries) {
            String nextNodeName = nextEntry.getKey();
            JsonNode nextNode = nextEntry.getValue();
            Set<String> nextNextNodeName = nodeEntrySetToNodeNameKeySet(nextNode.properties());
            if (nextNextNodeName.contains(input)) {
                return Optional.of(nextNodeName);
            }
        }
        return Optional.empty();
    }

    /*
     * Compare the input with the candidates and return the candidate that has a Levenshtein distance less than 3.
     */
    private Optional<String> suggestTypo(String input, Set<String> candidates) {
        LevenshteinDistance levenshteinDistance = new LevenshteinDistance();

        for (String candidate : candidates) {
            int distance = levenshteinDistance.apply(input, candidate);
            if (LOG.isDebugEnabled()) {
                LOG.debug("The Levenshtein distance between '{}' and '{}' is: {}", input, candidate, distance);
            }
            if (distance < 3) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    private Object concatNodeList(List<String> eventPathNodeList) {
        StringBuilder sb = new StringBuilder();
        for (String node : eventPathNodeList) {
            sb.append(node).append(".");
        }
        return sb.toString();
    }
}
