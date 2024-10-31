package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.drools.ansible.rulebook.integration.api.domain.Rule;
import org.drools.ansible.rulebook.integration.api.domain.RuleContainer;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.domain.conditions.MapCondition;
import org.kie.api.runtime.rule.Match;
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

    public static final String EVENT_STRUCTURE_SUGGESTION_ENABLED_ENV_NAME = "DROOLS_SUGGESTION_ENABLED";
    public static final String EVENT_STRUCTURE_SUGGESTION_ENABLED_PROPERTY = "drools.event.structure.suggestion.enabled";
    static boolean EVENT_STRUCTURE_SUGGESTION_ENABLED; // package-private for testing

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // key: event path split into nodes
    // value: list of rule names that use the event path
    private final Map<EventPath, List<String>> eventPathMap = new HashMap<>();

    // keep the first event json for validation
    // will be cleared after validation
    private String firstEventJson;

    /*
     * NO_EVENT -> FIRST_EVENT_EXECUTING -> VALIDATING (if the first event matches no rule) -> VALIDATED
     *                                   -> NO_NEED_TO_VALIDATE (if the first event matches any rule)
     */
    enum State {
        NO_EVENT,
        FIRST_EVENT_EXECUTING,
        VALIDATING,
        VALIDATED,
        NO_NEED_TO_VALIDATE
    }

    private State state = State.NO_EVENT;

    private String ruleSetName;

    static {
        String envValue = System.getenv(EVENT_STRUCTURE_SUGGESTION_ENABLED_ENV_NAME);
        if (envValue != null && !envValue.isEmpty()) {
            // Environment variable takes precedence over system property
            System.setProperty(EVENT_STRUCTURE_SUGGESTION_ENABLED_PROPERTY, envValue);
        }
        EVENT_STRUCTURE_SUGGESTION_ENABLED = Boolean.getBoolean(EVENT_STRUCTURE_SUGGESTION_ENABLED_PROPERTY);
    }

    public RulesSetEventStructure(RulesSet rulesSet) {
        try {
            if (EVENT_STRUCTURE_SUGGESTION_ENABLED) {
                analyzeRulesSet(rulesSet);
            }

            if (LOG.isTraceEnabled()) {
                LOG.trace("eventPathMap : {}", eventPathMap);
            }
        } catch (Exception e) {
            // catch all because this suggestion feature is not critical
            LOG.warn("Failed to analyze the rules set conditions. You may ignore this error because this feature doesn't affect the rule execution.", e);
            state = State.NO_NEED_TO_VALIDATE;
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
            if (key.equals("Event") && value instanceof String stringValue) {
                EventPath eventPath = new EventPath(splitEventPath(stringValue));
                eventPathMap.compute(eventPath, (k, v) -> {
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
     */
    private List<String> splitEventPath(String eventPath) {
        String[] nodes = eventPath.split("\\.");
        List<String> pathNodeList = new ArrayList<>();
        for (String node : nodes) {
            node = node.trim();
            // handle bracket notation
            int leftBracket = node.indexOf('[');
            if (leftBracket > 0) {
                int rightBracket = node.indexOf(']');
                String body = node.substring(0, leftBracket);
                String key = node.substring(leftBracket + 1, rightBracket);

                if (rightBracket < node.length() - 1) {
                    // This version doesn't analyze nested array or map access. (limitation)
                    LOG.debug("Multiple bracket pairs are not analyzed for structure suggestion. {}", node);
                    // return the path before the unparsed bracket
                    return pathNodeList;
                } else if (isQuotedString(key)) {
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

    /**
     * Store the first event json for validation if validation is required.
     */
    public void stashFirstEventJsonForValidation(String firstEventJson) {
        if (EVENT_STRUCTURE_SUGGESTION_ENABLED && state == State.NO_EVENT) {
            this.firstEventJson = firstEventJson;
            state = State.FIRST_EVENT_EXECUTING;
        }
    }

    /**
     * When the first event is executed and no rule is matched, validation takes place.
     * If the first event matches any rule, no longer need to validate.
     * If we don't have the first event, do nothing.
     */
    public void validateRulesSetEventStructureIfRequired(List<Match> matches) {
        if (EVENT_STRUCTURE_SUGGESTION_ENABLED && state == State.FIRST_EVENT_EXECUTING) {
            if (matches.isEmpty()) {
                validateRulesSetEventStructure();
            } else {
                state = State.NO_NEED_TO_VALIDATE;
                firstEventJson = null;
            }
        }
    }

    /**
     * Validate the event path in RuleSets using the incoming event structure.
     * Assume that the first event has the correct structure.
     */
    private void validateRulesSetEventStructure() {
        state = State.VALIDATING;

        try {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(firstEventJson);

            jsonNode = takeOneChildIfEventList(jsonNode);

            for (Map.Entry<EventPath, List<String>> eventPathEntry : eventPathMap.entrySet()) {
                validateEventPathWithEventStructure(eventPathEntry, jsonNode);
            }
        } catch (Exception e) {
            // catch all because this suggestion feature is not critical
            LOG.warn("Failed to validate the event structure. You may ignore this error because this feature doesn't affect the rule execution.", e);
        } finally {
            firstEventJson = null;
            state = State.VALIDATED;
        }
    }

    private JsonNode takeOneChildIfEventList(JsonNode jsonNode) {
        if (jsonNode.isObject() && jsonNode.size() == 1) {
            JsonNode events = jsonNode.get("events");
            if (events != null && events.isArray() && !events.isEmpty()) {
                return events.get(0);
            }
        }
        return jsonNode;
    }

    private void validateEventPathWithEventStructure(Map.Entry<EventPath, List<String>> eventPathEntry, JsonNode rootJsonNode) {
        List<String> eventPathNodeList = eventPathEntry.getKey().getEventPathNodeList();
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
                    // Limitation: assuming only one level of array.
                    currentJsonNode = currentJsonNode.get(0); // Limitation: assume that an array contains the same type values.
                    if (currentJsonNode.isObject()) {
                        currentJsonNode = currentJsonNode.get(trimBracket(currentPathNode));
                    } else if (currentJsonNode.isArray()) {
                        // Limitation: if this is an ArrayNode (= nested array), don't go deeper. No suggestion.
                        break;
                    } else {
                        // reached to a leaf node
                        break;
                    }
                }

                // set up nextKeyNames
                if (currentJsonNode.isObject()) {
                    nextKeyNames = nodeEntrySetToNodeNameKeySet(currentJsonNode.properties());
                } else if (currentJsonNode.isArray()) {
                    nextKeyNames = iteratorToSet(currentJsonNode.elements()).stream().flatMap(node -> nodeEntrySetToNodeNameKeySet(node.properties()).stream()).collect(toSet());
                    if (nextKeyNames.isEmpty()) {
                        // all children are leaf nodes
                        break;
                    }
                } else {
                    // reached to a leaf node
                    break;
                }
            } else {
                Optional<String> candidateForTypo = suggestTypo(currentPathNode, nextKeyNames);
                if (candidateForTypo.isPresent()) {
                    LOG.warn("'{}' in the condition '{}' in rule set '{}' rule {} does not meet with the incoming event property {}. Did you mean '{}'?",
                             currentPathNode, concatNodeList(eventPathNodeList), ruleSetName, ruleNames, nextKeyNames, candidateForTypo.get());
                }

                Optional<String> candidateForMissingNode = suggestMissingNode(currentPathNode, currentJsonNode);
                if (candidateForMissingNode.isPresent()) {
                    LOG.warn("'{}' in the condition '{}' in rule set '{}' rule {} does not meet with the incoming event property {}. Did you forget to include '{}'?",
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
        return entrySet.stream()
                .map(entry -> addBracketIfArrayNode(entry.getKey(), entry.getValue()))
                .collect(toSet());
    }

    private static Set<JsonNode> iteratorToSet(Iterator<JsonNode> iterator) {
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
            Set<String> nextNextNodeName;
            if (nextNode.isObject()) {
                nextNextNodeName = nodeEntrySetToNodeNameKeySet(nextNode.properties());
            } else if (nextNode.isArray()) {
                nextNextNodeName = iteratorToSet(nextNode.elements()).stream().flatMap(node -> nodeEntrySetToNodeNameKeySet(node.properties()).stream()).collect(toSet());
            } else {
                // reached to a leaf node
                continue;
            }
            if (nextNextNodeName.contains(input)) {
                return Optional.of(addBracketIfArrayNode(nextNodeName, nextNode));
            }
        }
        return Optional.empty();
    }

    private static String addBracketIfArrayNode(String nextNodeName, JsonNode nextNode) {
        if (nextNode.isArray()) {
            return nextNodeName + "[]";
        } else {
            return nextNodeName;
        }
    }

    /*
     * Compare the input with the candidates and return the candidate that has a Levenshtein distance less than 3.
     */
    private Optional<String> suggestTypo(String input, Set<String> candidates) {
        LevenshteinDistance levenshteinDistance = new LevenshteinDistance();

        for (String candidate : candidates) {
            if (isArrayWithoutBracket(input, candidate)) {
                // array without bracket is an acceptable syntax: DROOLS-7639
                return Optional.empty();
            }
            int distance = levenshteinDistance.apply(input, candidate);
            if (LOG.isTraceEnabled()) {
                LOG.trace("The Levenshtein distance between '{}' and '{}' is: {}", input, candidate, distance);
            }
            if (distance < 3) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    private boolean isArrayWithoutBracket(String input, String candidate) {
        return candidate.endsWith("[]") && input.equals(candidate.substring(0, candidate.length() - 2));
    }

    private String concatNodeList(List<String> eventPathNodeList) {
        return String.join(".", eventPathNodeList);
    }

    static class EventPath {
        private final List<String> eventPathNodeList;
        private final int hashCode;

        public EventPath(List<String> eventPathNodeList) {
            this.eventPathNodeList = List.copyOf(eventPathNodeList); // immutable
            this.hashCode = Objects.hash(this.eventPathNodeList);
        }

        public List<String> getEventPathNodeList() {
            return eventPathNodeList;
        }

        @Override
        public String toString() {
            return "EventPath{" +
                    "eventPathNodeList=" + eventPathNodeList +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            EventPath that = (EventPath) o;
            return Objects.equals(eventPathNodeList, that.eventPathNodeList);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
