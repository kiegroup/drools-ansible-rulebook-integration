package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests for AstRulesEngine with HA functionality.
 * This test uses RULE_SET_BASIC for basic use cases.
 * To test other rules, create another test class extending HAIntegrationTestBase
 */
class HAIntegrationTest extends HAIntegrationTestBase {

    // Basic rule
    private static final String RULE_SET_BASIC = """
                {
                    "name": "Test Ruleset",
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
                                        "name": "send_alert.yml",
                                        "extra_vars": {
                                            "message": "High temperature detected"
                                        }
                                    }
                                ]
                            }
                        }}
                    ]
                }
                """;

    protected String getRuleSet() {
        return RULE_SET_BASIC;
    }

    private static final String RULE_SET_SECONDARY = """
            {
                "name": "Humidity Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "humidity_alert",
                        "condition": {
                            "GreaterThanExpression": {
                                "lhs": {
                                    "Event": "humidity"
                                },
                                "rhs": {
                                    "Integer": 70
                                }
                            }
                        },
                        "action": {
                            "run_playbook": [
                                {
                                    "name": "humid_alert.yml",
                                    "extra_vars": {
                                        "message": "High humidity detected"
                                    }
                                }
                            ]
                        }
                    }}
                ]
            }
            """;

    private String getSecondaryRuleSetName() {
        return (String) JsonMapper.readValueAsMapOfStringAndObject(RULE_SET_SECONDARY).get("name");
    }

    @Test
    void testBasicBehaviorOtherThanHA() {
        // Enable leader mode
        rulesEngine1.enableLeader();

        // Process an event that triggers a rule
        String event = createEvent("""
                {
                    "temperature": 35,
                    "timestamp": "2024-01-01T10:00:00Z"
                }
                """);

        String result = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(result).isNotNull();

        // Parse result to verify ME UUID is included
        List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
        assertThat(matchList).hasSize(1);

        Map<String, Object> matchJson = matchList.get(0);
        assertThat(matchJson.get("name")).isEqualTo("temperature_alert");

        Map<String, Object> events = (Map<String, Object>) matchJson.get("events");
        assertThat(events).containsKey("m"); // Original match data

        String meUuid = (String) matchJson.get("matching_uuid");
        assertThat(meUuid).isNotEmpty();
        assertThat(meUuid).hasSize(36); // UUID format check

        // Get session stats to verify rule triggered
        String stats = rulesEngine1.sessionStats(sessionId1);
        assertThat(stats).isNotNull();
        Map<String, Object> statsMap = JsonMapper.readValueAsMapOfStringAndObject(stats);
        assertThat(statsMap.get("rulesTriggered")).isEqualTo(1);
    }

    @Test
    void testMatchingEventPersistence() {
        rulesEngine1.enableLeader();

        // Process an event that triggers a rule
        String event = createEvent("""
                {
                    "temperature": 35,
                    "sensor": "temp_01"
                }
                """);

        String result = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(result).isNotNull();
        String meUuid = TestUtils.extractMatchingUuidFromResponse(result);

        // Verify that matching events were persisted to database
        // We can check this by accessing another HAStateManager directly, so this is a relatively white-box test
        HAStateManager haStateManagerForAssertion = createHAStateManagerForAssertion();

        try {
            List<MatchingEvent> pendingEvents = haStateManagerForAssertion.getPendingMatchingEvents();
            assertThat(pendingEvents).isNotEmpty();

            MatchingEvent me = pendingEvents.get(0);
            assertThat(me.getRuleName()).isEqualTo("temperature_alert");
            assertThat(me.getMeUuid()).isEqualTo(meUuid);
        } finally {
            haStateManagerForAssertion.shutdown();
        }
    }

    @Test
    void testCurrentStateSha() {
        // Scenario: Create ruleset (done by @BeforeEach) -> Become leader
        rulesEngine1.enableLeader();

        String eventUuid1 = "11111111-2222-3333-4444-555555555555";
        String event1 = """
                {
                    "meta": {"uuid": "%s"},
                    "temperature": 35
                }
                """.formatted(eventUuid1);

        String result1 = rulesEngine1.assertEvent(sessionId1, event1); // matches the rule. the event is discarded
        assertThat(result1).contains("temperature_alert");

        HAStateManager haManagerForAssertion = createHAStateManagerForAssertion();
        SessionState state1 = haManagerForAssertion.getPersistedSessionState(getRuleSetNameValue());

        assertThat(state1).isNotNull();
        // SHA is calculated from complete state content
        assertThat(state1.getCurrentStateSHA()).isNotNull();
        assertThat(state1.getPartialEvents()).isEmpty();

        // Verify integrity by recalculating SHA
        String recalculatedSha1 = HAUtils.calculateStateSHA(state1);
        assertThat(state1.getCurrentStateSHA()).isEqualTo(recalculatedSha1);

        String eventUuid2 = "XXXXXXXX-2222-3333-4444-555555555555";
        String event2 = """
                {
                    "meta": {"uuid": "%s"},
                    "no-match": 35
                }
                """.formatted(eventUuid2);

        String result2 = rulesEngine1.assertEvent(sessionId1, event2); // doesn't match the rule. the event is also discarded
        assertThat(result2).doesNotContain("temperature_alert");

        SessionState state2 = haManagerForAssertion.getPersistedSessionState(getRuleSetNameValue());

        assertThat(state2).isNotNull();

        assertThat(state2.getCurrentStateSHA()).isNotNull();
        assertThat(state1.getPartialEvents()).isEmpty();

        // Verify integrity by recalculating SHA
        String recalculatedSha2 = HAUtils.calculateStateSHA(state2);
        assertThat(state2.getCurrentStateSHA()).isEqualTo(recalculatedSha2);
    }

    @Test
    void testActionStateManagement() {
        rulesEngine1.enableLeader();

        // First trigger a rule to create a matching event
        String event = createEvent("""
                {
                    "temperature": 35,
                    "location": "server_room"
                }
                """);

        String result = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(result).isNotNull();

        // Simulate that the Python side extracts the ME UUID from the result
        String meUuid = TestUtils.extractMatchingUuidFromResponse(result);
        assertThat(meUuid).isNotNull();

        // Set ActionInfo
        String actionData = "{\"name\":\"send_alert\",\"status\":4,\"reference_id\":\"job-456\"}";
        rulesEngine1.addActionInfo(sessionId1, meUuid, 0, actionData);
        assertThat(rulesEngine1.getActionStatus(sessionId1, meUuid, 0)).isEqualTo("4");

        // Check action exists and get it
        assertThat(rulesEngine1.actionInfoExists(sessionId1, meUuid, 0)).isTrue();
        String retrieved = rulesEngine1.getActionInfo(sessionId1, meUuid, 0);
        assertThat(retrieved).isEqualTo(actionData);

        // Update ActionInfo
        String updatedActionData = "{\"name\":\"send_alert\",\"status\":3,\"reference_id\":\"job-456\"}";
        rulesEngine1.updateActionInfo(sessionId1, meUuid, 0, updatedActionData);
        assertThat(rulesEngine1.getActionStatus(sessionId1, meUuid, 0)).isEqualTo("3");

        // Delete ActionInfo and MarchingEvent when complete
        rulesEngine1.deleteActionInfo(sessionId1, meUuid);

        // Should not exist after deletion
        assertThat(rulesEngine1.actionInfoExists(sessionId1, meUuid, 0)).isFalse();
    }



    @Test
    void testLeaderTransitionWithRecovery() {
        // Set as leader
        rulesEngine1.enableLeader();

        // Process some events
        String event = createEvent("""
                {
                    "temperature": 35,
                    "zone": "production"
                }
                """);
        rulesEngine1.assertEvent(sessionId1, event);

        // Simulate leader failure - unset leader
        rulesEngine1.disableLeader();

        // New leader takes over. Recovery occurs.
        rulesEngine2.enableLeader();

        // Verify we can still process events
        String event2 = createEvent("""
                {
                    "temperature": 40,
                    "zone": "production"
                }
                """);
        String result2 = rulesEngine2.assertEvent(sessionId2, event2);
        assertThat(result2).isNotNull();

        // Check that both events created matching events
        HAStateManager haStateManagerForAssertion = createHAStateManagerForAssertion();

        try {
            List<MatchingEvent> pendingEvents = haStateManagerForAssertion.getPendingMatchingEvents();
            assertThat(pendingEvents).hasSize(2); // Both events should create MEs
        } finally {
            haStateManagerForAssertion.shutdown();
        }
    }

    @Test
    void testAsyncMessageReceiveOnFailoverRecovery() {
        rulesEngine1.enableLeader();

        // Process 2 events
        String event1 = createEvent("""
                {
                    "temperature": 45,
                    "critical": true
                }
                """);
        String result1 = rulesEngine1.assertEvent(sessionId1, event1);
        String meUuid = TestUtils.extractMatchingUuidFromResponse(result1);
        assertThat(meUuid).isNotNull();

        String event2 = createEvent("""
                {
                    "temperature": 55,
                    "critical": true
                }
                """);
       rulesEngine1.assertEvent(sessionId1, event2);


        // Simulate engine-1 failure
        rulesEngine1.disableLeader();

        // enableLeader triggers sending pending matches to async channel
        rulesEngine2.enableLeader();

        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> consumer2.getReceivedMessages().size() >= 1);

        String asyncResult = consumer2.getReceivedMessages().get(0);

        Map<String, Object> asyncResultMap = readValueAsMapOfStringAndObject(asyncResult);

        System.out.println("Async Result Map: " + asyncResultMap);

        assertThat(asyncResultMap).isNotNull();
        assertThat(asyncResultMap).containsKey("session_id");
        List<Map<String, Object>> resultList = (List<Map<String, Object>>) asyncResultMap.get("result");

        Map<String, Object> matchingEvent1 = resultList.get(0);
        assertThat(matchingEvent1).containsEntry("matching_uuid", meUuid);
        assertThat(matchingEvent1).containsEntry("ruleset_name", "Test Ruleset");
        assertThat(matchingEvent1).containsEntry("name", "temperature_alert");
        Map<String, Map> events1 = (Map<String, Map>) matchingEvent1.get("events");
        assertThat(events1.get("m")).containsEntry("critical", true);
        assertThat(events1.get("m")).containsEntry("temperature", 45);

        Map<String, Object> matchingEvent2 = resultList.get(1);
        Map<String, Map> events2 = (Map<String, Map>) matchingEvent2.get("events");
        assertThat(events2.get("m")).containsEntry("temperature", 55);

        // result is sorted by created_at
        assertThat(((Long) matchingEvent2.get("created_at")).longValue())
                .isGreaterThanOrEqualTo(((Long) matchingEvent1.get("created_at")).longValue());
    }

    @Test
    void testGetHAStats() throws Exception {
        // Check initial stats
        String statsJson = rulesEngine1.getHAStats();
        Map<String, Object> stats = readValueAsMapOfStringAndObject(statsJson);
        assertThat(stats).isNotNull();
        assertThat(stats.get("current_leader")).isNull();
        assertThat(stats.get("leader_switches")).isEqualTo(0);
        assertThat(stats.get("events_processed_in_term")).isEqualTo(0);
        assertThat(stats.get("actions_processed_in_term")).isEqualTo(0);
        assertThat(stats.get("incomplete_matching_events")).isEqualTo(0);
        assertThat(((Number) stats.get("partial_events_in_memory")).intValue()).isZero();
        assertThat(((Number) stats.get("partial_fulfilled_rules")).intValue()).isGreaterThanOrEqualTo(0);

        // Enable leader
        rulesEngine1.enableLeader();

        statsJson = rulesEngine1.getHAStats();
        stats = readValueAsMapOfStringAndObject(statsJson);
        assertThat(stats.get("current_leader")).isEqualTo("worker-1");
        assertThat(stats.get("leader_switches")).isEqualTo(1);
        assertThat(stats.get("current_term_started_at")).isNotNull();

        // Process an event and action
        String result = rulesEngine1.assertEvent(sessionId1, createEvent("{\"temperature\": 35}"));
        String meUuid = TestUtils.extractMatchingUuidFromResponse(result);

        rulesEngine1.addActionInfo(sessionId1, meUuid, 0, "{\"name\":\"test\",\"status\":\"running\"}");

        statsJson = rulesEngine1.getHAStats();
        stats = readValueAsMapOfStringAndObject(statsJson);
        assertThat(stats.get("events_processed_in_term")).isEqualTo(1); // TODO: implement event count increment on assertEvent
        assertThat(stats.get("actions_processed_in_term")).isEqualTo(1);
        assertThat(stats.get("incomplete_matching_events")).isEqualTo(1);
        assertThat(((Number) stats.get("partial_events_in_memory")).intValue()).isZero();
        assertThat(((Number) stats.get("partial_fulfilled_rules")).intValue()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testHAStatsAcrossFailoverAggregatesGlobalSessionStats() throws Exception {
        // Node1 becomes leader and processes two events
        rulesEngine1.enableLeader();
        rulesEngine1.assertEvent(sessionId1, createEvent("{\"temperature\": 35}"));
        rulesEngine1.assertEvent(sessionId1, createEvent("{\"temperature\": 36}"));

        String statsJsonLeader1 = rulesEngine1.getHAStats();
        Map<String, Object> statsLeader1 = readValueAsMapOfStringAndObject(statsJsonLeader1);
        Map<String, Object> globalStatsLeader1 = (Map<String, Object>) statsLeader1.get("global_session_stats");
        assertThat(globalStatsLeader1).isNotNull();
        assertThat(globalStatsLeader1).containsEntry("eventsProcessed", 2)
                .containsEntry("rulesTriggered", 2);
        String firstStart = (String) globalStatsLeader1.get("start");

        // Simulate failover: leader1 down, leader2 up
        rulesEngine1.disableLeader();
        rulesEngine2.enableLeader();

        // Leader2 processes one more event
        rulesEngine2.assertEvent(sessionId2, createEvent("{\"temperature\": 37}"));

        String statsJsonLeader2 = rulesEngine2.getHAStats();
        Map<String, Object> statsLeader2 = readValueAsMapOfStringAndObject(statsJsonLeader2);
        assertThat(statsLeader2.get("current_leader")).isEqualTo("worker-2");
        Map<String, Object> globalStatsLeader2 = (Map<String, Object>) statsLeader2.get("global_session_stats");
        assertThat(globalStatsLeader2).isNotNull();

        // start should remain the first leader's start
        assertThat(globalStatsLeader2.get("start")).isEqualTo(firstStart);
        // counters should accumulate across leaders
        assertThat(globalStatsLeader2).containsEntry("eventsProcessed", 3)
                .containsEntry("rulesTriggered", 3);
    }

    @Test
    void testMultipleRuleSets() {
        rulesEngine1.enableLeader();

        long sessionIdPrimaryNode1 = sessionId1;
        long sessionIdSecondaryNode1 = rulesEngine1.createRuleset(RULE_SET_SECONDARY);

        long sessionIdPrimaryNode2 = sessionId2;
        long sessionIdSecondaryNode2 = rulesEngine2.createRuleset(RULE_SET_SECONDARY); // prepare before failover

        // Process events that hit both rulesets before the crash
        String eventPrimary = """
                {
                    "meta": {"uuid": "aaaaaaaa-1111-2222-3333-444444444444"},
                    "temperature": 45,
                    "critical": true
                }
                """;
        String primaryResult = rulesEngine1.assertEvent(sessionIdPrimaryNode1, eventPrimary);
        String primaryUuid = TestUtils.extractMatchingUuidFromResponse(primaryResult);
        assertThat(primaryUuid).isNotNull();

        String eventSecondary = """
                {
                    "meta": {"uuid": "bbbbbbbb-1111-2222-3333-555555555555"},
                    "humidity": 80,
                    "location": "greenhouse"
                }
                """;
        String secondaryResult = rulesEngine1.assertEvent(sessionIdSecondaryNode1, eventSecondary);
        String secondaryUuid = TestUtils.extractMatchingUuidFromResponse(secondaryResult);
        assertThat(secondaryUuid).isNotNull();

        // Simulate crash of the leader handling both rule sets
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();

        // Fail-over
        rulesEngine2.enableLeader();

        // Process new events in each ruleset to ensure they continue to function
        List<Map<String, Object>> replayPrimary = JsonMapper.readValueAsListOfMapOfStringAndObject(
                rulesEngine2.assertEvent(sessionIdPrimaryNode2, "{\"meta\":{\"uuid\":\"cccccccc-1111-2222-3333-666666666666\"},\"temperature\":48}"));
        assertThat(replayPrimary.get(0).get("matching_uuid")).isNotNull();

        List<Map<String, Object>> replaySecondary = JsonMapper.readValueAsListOfMapOfStringAndObject(
                rulesEngine2.assertEvent(sessionIdSecondaryNode2, "{\"meta\":{\"uuid\":\"dddddddd-1111-2222-3333-777777777777\"},\"humidity\":75}"));
        assertThat(replaySecondary.get(0).get("matching_uuid")).isNotNull();

        // Confirm the database still tracks the two rule sets independently
        HAStateManager stateManagerForAssertion = createHAStateManagerForAssertion();
        try {
            SessionState statePrimary = stateManagerForAssertion.getPersistedSessionState(getRuleSetNameValue());
            SessionState stateSecondary = stateManagerForAssertion.getPersistedSessionState(getSecondaryRuleSetName());
            assertThat(statePrimary.getRuleSetName()).isEqualTo(getRuleSetNameValue());
            assertThat(stateSecondary.getRuleSetName()).isEqualTo(getSecondaryRuleSetName());
        } finally {
            stateManagerForAssertion.shutdown();
        }
    }

    @Test
    void testHAOperationsWithoutConfiguration() {
        try (AstRulesEngine rulesEngine = new AstRulesEngine()) {
            rulesEngine.createRuleset(RULE_SET_BASIC);
            assertThrows(IllegalStateException.class, () -> {
                rulesEngine.enableLeader();
            });
        }
    }
}
