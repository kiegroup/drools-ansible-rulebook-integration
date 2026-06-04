package org.drools.ansible.rulebook.integration.ha.tests.integration.basic;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for duplicate event detection via circular buffer of processed event IDs.
 */
class HADuplicateEventDetectionTest extends HAIntegrationTestBase {

    private static final String RULE_SET = """
            {
                "name": "Dedup Test Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "temperature_alert",
                        "condition": {
                            "GreaterThanExpression": {
                                "lhs": {
                                    "Event": "temperature"
                                },
                                "rhs": {
                                    "Integer": 30
                                }
                            }
                        },
                        "action": {
                            "run_playbook": [
                                {
                                    "name": "send_alert.yml"
                                }
                            ]
                        }
                    }}
                ]
            }
            """;

    @Override
    protected String getRuleSet() {
        return RULE_SET;
    }

    @Test
    void testDuplicateEventIgnored() {
        rulesEngine1.enableLeader();

        String eventUuid = "aaaaaaaa-1111-2222-3333-444444444444";
        String event = """
                {
                    "meta": {"uuid": "%s"},
                    "temperature": 35
                }
                """.formatted(eventUuid);

        // First send should match
        String result1 = rulesEngine1.assertEvent(sessionId1, event);
        List<Map<String, Object>> matchList1 = JsonMapper.readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matchList1).hasSize(1);
        assertThat(matchList1.get(0).get("name")).isEqualTo("temperature_alert");

        // Second send of same event should be ignored (duplicate)
        String result2 = rulesEngine1.assertEvent(sessionId1, event);
        List<Map<String, Object>> matchList2 = JsonMapper.readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matchList2).isEmpty();
    }

    @Test
    void testBufferEviction() {
        rulesEngine1.enableLeader();

        // Default buffer size is 5. Send 6 unique events.
        List<String> eventUuids = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            String uuid = "eviction-" + i + "-1111-2222-3333-444444444444";
            eventUuids.add(uuid);
            String event = """
                    {
                        "meta": {"uuid": "%s"},
                        "temperature": 35
                    }
                    """.formatted(uuid);
            rulesEngine1.assertEvent(sessionId1, event);
        }

        // The oldest event (index 0) should have been evicted from the buffer.
        // Resending it should be treated as a new event (match again).
        String oldestEvent = """
                {
                    "meta": {"uuid": "%s"},
                    "temperature": 35
                }
                """.formatted(eventUuids.get(0));
        String result = rulesEngine1.assertEvent(sessionId1, oldestEvent);
        List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
        assertThat(matchList).hasSize(1);

        // The most recent event (index 5) should still be in the buffer.
        // Resending it should be ignored.
        String newestEvent = """
                {
                    "meta": {"uuid": "%s"},
                    "temperature": 35
                }
                """.formatted(eventUuids.get(5));
        String result2 = rulesEngine1.assertEvent(sessionId1, newestEvent);
        List<Map<String, Object>> matchList2 = JsonMapper.readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matchList2).isEmpty();
    }

    @Test
    void testNonUuidEventIds() {
        rulesEngine1.enableLeader();

        // Use a Kafka-style opaque string ID
        String kafkaStyleId = "my_kafka:my_partition:my_topic:1";
        String event = """
                {
                    "meta": {"uuid": "%s"},
                    "temperature": 35
                }
                """.formatted(kafkaStyleId);

        // First send should match
        String result1 = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(JsonMapper.readValueAsListOfMapOfStringAndObject(result1)).hasSize(1);

        // Duplicate should be ignored
        String result2 = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(JsonMapper.readValueAsListOfMapOfStringAndObject(result2)).isEmpty();

        // Use a SHA256-style ID
        String sha256Id = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        String event2 = """
                {
                    "meta": {"uuid": "%s"},
                    "temperature": 40
                }
                """.formatted(sha256Id);

        // First send should match
        String result3 = rulesEngine1.assertEvent(sessionId1, event2);
        assertThat(JsonMapper.readValueAsListOfMapOfStringAndObject(result3)).hasSize(1);

        // Duplicate should be ignored
        String result4 = rulesEngine1.assertEvent(sessionId1, event2);
        assertThat(JsonMapper.readValueAsListOfMapOfStringAndObject(result4)).isEmpty();
    }

    @Test
    void testDuplicateDetectionSurvivesFailover() {
        // Node1 processes event
        rulesEngine1.enableLeader();

        String eventUuid = "failover-1111-2222-3333-444444444444";
        String event = """
                {
                    "meta": {"uuid": "%s"},
                    "temperature": 35
                }
                """.formatted(eventUuid);

        String result1 = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(JsonMapper.readValueAsListOfMapOfStringAndObject(result1)).hasSize(1);

        // Verify processedEventIds persisted to DB
        HAStateManager haManagerForAssertion = createHAStateManagerForAssertion();
        try {
            SessionState state = haManagerForAssertion.getPersistedSessionState(getRuleSetNameValue());
            assertThat(state).isNotNull();
            assertThat(state.getProcessedEventIds()).contains(eventUuid);
        } finally {
            haManagerForAssertion.shutdown();
        }

        // Failover: node1 down, node2 takes over
        rulesEngine1.disableLeader();
        rulesEngine2.enableLeader();

        // Re-send same event to node2 - should be ignored
        String result2 = rulesEngine2.assertEvent(sessionId2, event);
        List<Map<String, Object>> matchList2 = JsonMapper.readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matchList2).isEmpty();

        // A different event should still be processed
        String newEvent = """
                {
                    "meta": {"uuid": "new-event-2222-3333-4444-555555555555"},
                    "temperature": 40
                }
                """;
        String result3 = rulesEngine2.assertEvent(sessionId2, newEvent);
        assertThat(JsonMapper.readValueAsListOfMapOfStringAndObject(result3)).hasSize(1);
    }
}
