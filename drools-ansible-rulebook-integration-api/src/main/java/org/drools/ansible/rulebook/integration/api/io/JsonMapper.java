package org.drools.ansible.rulebook.integration.api.io;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonMapper {

    private static final TypeReference<List<Map<String, Object>>> LIST_OF_MAP_OF_STRING_AND_OBJECT = new TypeReference<List<Map<String, Object>>>(){};
    private static final TypeReference<Map<String, Object>> MAP_OF_STRING_AND_OBJECT = new TypeReference<Map<String, Object>>(){};
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static { // most tests contains non-standard JSON; eventually check assumption non-strict json with Ansible team
        OBJECT_MAPPER.enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES.mappedFeature());
        OBJECT_MAPPER.enable(JsonReadFeature.ALLOW_SINGLE_QUOTES.mappedFeature());
    }
    private static final JavaType JACKSON_RAW_LIST = OBJECT_MAPPER.getTypeFactory().constructRawCollectionLikeType(List.class);

    public static String toJson(Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<Object> readValueExtractFieldAsList(String json, String field) {
        try {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(json).get(field);
            if (jsonNode == null) {
                throw new FieldNotPresentException(field);
            }
            return OBJECT_MAPPER.treeToValue(jsonNode, JACKSON_RAW_LIST);
        } catch (JacksonException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class FieldNotPresentException extends RuntimeException {
        public FieldNotPresentException(String field) {
            super("could not find field '"+field+"' in the JSON.");
        }
    }

    public static List<Map<String, Object>> readValueAsListOfMapOfStringAndObject(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, LIST_OF_MAP_OF_STRING_AND_OBJECT);
        } catch (JacksonException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Map<String, Object> readValueAsMapOfStringAndObject(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, MAP_OF_STRING_AND_OBJECT);
        } catch (JacksonException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<Object> readValueAsListOfObject(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, JACKSON_RAW_LIST);
        } catch (JacksonException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Object readValueAsRawObject(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, Object.class);
        } catch (JacksonException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Object readValueAtAsRawObject(String json, String jsonPointer) {
        try {
            JsonNode tree = OBJECT_MAPPER.readTree(json);
            JsonNode at = tree.at(jsonPointer);
            return OBJECT_MAPPER.treeToValue(at, Object.class);
        } catch (JacksonException e) {
            throw new UncheckedIOException(e);
        }
    }
}
