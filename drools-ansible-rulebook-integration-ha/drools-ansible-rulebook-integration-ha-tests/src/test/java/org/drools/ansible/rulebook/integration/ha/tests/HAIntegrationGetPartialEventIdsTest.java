package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfObject;

/**
 * Integration tests for getPartialEventIds API across failover.
 */
class HAIntegrationGetPartialEventIdsTest extends HAIntegrationTestBase {

    // Multi condition rule: requires both i=1 AND j=2 to fire.
    // Asserting only one condition creates a partial event in working memory.
    private static final String RULE_SET = """
                {
                    "name": "PartialEventIds Ruleset",
                    "rules": [
                        {"Rule": {
                            "name": "two_condition_rule",
                            "condition": {
                                "AllCondition": [
                                    {
                                        "EqualsExpression": {
                                            "lhs": {"Event": "i"},
                                            "rhs": {"Integer": 1}
                                        }
                                    },
                                    {
                                        "EqualsExpression": {
                                            "lhs": {"Event": "j"},
                                            "rhs": {"Integer": 2}
                                        }
                                    }
                                ]
                            },
                            "action": {
                                "run_playbook": [
                                    {
                                        "name": "alert.yml"
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

    private static String createEventWithUuid(String eventBody, String uuid) {
        return eventBody.replaceFirst("\\{", "{\"meta\": {\"uuid\": \"" + uuid + "\"}, ");
    }

    @Test
    void testGetPartialEventIds_emptyWhenNoEvents() {
        rulesEngine1.enableLeader();

        String result = rulesEngine1.getPartialEventIds(sessionId1);
        assertThat(readValueAsListOfObject(result)).isEmpty();
    }

    @Test
    void testGetPartialEventIds_returnsIdAfterPartialEvent() {
        rulesEngine1.enableLeader();

        // Assert one event satisfying only the first condition -> stays partial
        String eventUuid = UUID.randomUUID().toString();
        String event = createEventWithUuid("{\"i\":1}", eventUuid);
        String matchResult = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(readValueAsListOfMapOfStringAndObject(matchResult)).isEmpty();

        // getPartialEventIds should return the event's UUID
        String idsJson = rulesEngine1.getPartialEventIds(sessionId1);
        List<Object> ids = readValueAsListOfObject(idsJson);
        assertThat(ids).hasSize(1);
        assertThat(ids).containsExactly(eventUuid);
    }

    @Test
    void testGetPartialEventIds_multiplePartialEvents() {
        rulesEngine1.enableLeader();

        // Assert two events, each satisfying only one condition
        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        rulesEngine1.assertEvent(sessionId1, createEventWithUuid("{\"i\":1}", uuid1));
        rulesEngine1.assertEvent(sessionId1, createEventWithUuid("{\"i\":1}", uuid2));

        String idsJson = rulesEngine1.getPartialEventIds(sessionId1);
        List<Object> ids = readValueAsListOfObject(idsJson);
        assertThat(ids).hasSize(2);
        assertThat(ids).containsExactlyInAnyOrder(uuid1, uuid2);
    }

    @Test
    void testGetPartialEventIds_clearedAfterRuleFires() {
        rulesEngine1.enableLeader();

        // Assert first event (partial)
        String uuid1 = UUID.randomUUID().toString();
        rulesEngine1.assertEvent(sessionId1, createEventWithUuid("{\"i\":1}", uuid1));

        assertThat(readValueAsListOfObject(rulesEngine1.getPartialEventIds(sessionId1))).hasSize(1);

        // Assert second event that completes the rule match
        String uuid2 = UUID.randomUUID().toString();
        String matchResult = rulesEngine1.assertEvent(sessionId1, createEventWithUuid("{\"j\":2}", uuid2));
        assertThat(readValueAsListOfMapOfStringAndObject(matchResult)).hasSize(1);

        // Both events consumed by rule firing -> no more partial events
        String idsJson = rulesEngine1.getPartialEventIds(sessionId1);
        assertThat(readValueAsListOfObject(idsJson)).isEmpty();
    }

    @Test
    void testGetPartialEventIds_survivesFailover() {
        // Step 1: Node 1 becomes leader and creates partial events
        rulesEngine1.enableLeader();

        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        rulesEngine1.assertEvent(sessionId1, createEventWithUuid("{\"i\":1}", uuid1));
        rulesEngine1.assertEvent(sessionId1, createEventWithUuid("{\"i\":1}", uuid2));

        // Verify partial events on node 1
        List<Object> idsBeforeFailover = readValueAsListOfObject(rulesEngine1.getPartialEventIds(sessionId1));
        assertThat(idsBeforeFailover).hasSize(2);
        assertThat(idsBeforeFailover).containsExactlyInAnyOrder(uuid1, uuid2);

        // Step 2: Simulate node 1 crash
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Step 3: Node 2 takes over (recovers session from database)
        rulesEngine2.enableLeader();

        // Step 4: Verify partial events recovered on node 2
        List<Object> idsAfterFailover = readValueAsListOfObject(rulesEngine2.getPartialEventIds(sessionId2));
        assertThat(idsAfterFailover).hasSize(2);
        assertThat(idsAfterFailover).containsExactlyInAnyOrder(uuid1, uuid2);
    }

    @Test
    void testGetPartialEventIds_failoverThenComplete() {
        // Step 1: Node 1 creates a partial event
        rulesEngine1.enableLeader();

        String uuid1 = UUID.randomUUID().toString();
        rulesEngine1.assertEvent(sessionId1, createEventWithUuid("{\"i\":1}", uuid1));

        assertThat(readValueAsListOfObject(rulesEngine1.getPartialEventIds(sessionId1)))
                .containsExactly(uuid1);

        // Step 2: Failover to node 2
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        rulesEngine2.enableLeader();

        // Step 3: Verify partial event recovered
        assertThat(readValueAsListOfObject(rulesEngine2.getPartialEventIds(sessionId2)))
                .containsExactly(uuid1);

        // Step 4: Complete the rule on node 2
        String uuid2 = UUID.randomUUID().toString();
        String matchResult = rulesEngine2.assertEvent(sessionId2, createEventWithUuid("{\"j\":2}", uuid2));
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(matchResult);
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0)).containsEntry("name", "two_condition_rule");

        // Step 5: Partial events should be cleared after rule fires
        assertThat(readValueAsListOfObject(rulesEngine2.getPartialEventIds(sessionId2))).isEmpty();
    }
}
