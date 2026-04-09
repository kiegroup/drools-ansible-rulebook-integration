package org.drools.ansible.rulebook.integration.ha.tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.calculateStateSHA;

/**
 * Session related tests for HAStateManager
 */
class HAStateManagerSessionTest extends HAStateManagerTestBase {

    private HAStateManager stateManager;
    private static final String HA_UUID = "test-ha-1";
    private static final String LEADER_ID = "test-leader-1";
    private static final String RULE_SET_NAME = "Test Ruleset";

    @BeforeEach
    void setUp() {
        stateManager = HAStateManagerFactory.create(TEST_DB_TYPE);
        stateManager.initializeHA(HA_UUID, LEADER_ID, dbParams, dbHAConfig);
    }

    @AfterEach
    void tearDown() {
        if (stateManager != null) {
            stateManager.shutdown();
        }

        cleanupDatabase();
    }

    // Revisit this test. Scenario is:
    // 1. Node1 writes initial state
    // 2. Node1 crashes before completing an update
    // 3. On recovery, Node2 becomes leader and reads the state
    // 4. Node2 can compare with its own state. They are the same, so no action needed
    // Note: This is more of an integration test scenario. This unit test should be much simpler.
    @Test
    void testTwoVersionState() {
        // TBD
    }

    public static final String ALL_CONDITION_RULE =
            """
            {
                "name": "Test Ruleset",
                "rules": [
                        {
                            "Rule": {
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "i"
                                                },
                                                "rhs": {
                                                    "Integer": 1
                                                }
                                            }
                                        },
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "j"
                                                },
                                                "rhs": {
                                                    "Integer": 2
                                                }
                                            }
                                        }
                                    ]
                                },
                                "enabled": true,
                                "name": null
                            }
                        }
                    ]
            }
            """;

    @Test
    void testSessionRecoveryWithPartialMatch() {
        stateManager.enableLeader();

        // This test works without HARulesExecutor
        RulesExecutor rulesExecutor1 = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK), ALL_CONDITION_RULE);
        long createdTime = rulesExecutor1.asKieSession().getSessionClock().getCurrentTime();

        rulesExecutor1.advanceTime(10, java.util.concurrent.TimeUnit.SECONDS);

        String eventJson = "{\"i\":1}";
        long insertedAt = createdTime + 10 * 1000; // 10 seconds later

        EventRecord event1 = new EventRecord(eventJson, insertedAt, EventRecord.RecordType.EVENT);
        List<EventRecord> partialEvents = List.of(event1);

        List<Match> matchList = rulesExecutor1.processEvents(eventJson).join(); // partial match
        assertThat(matchList).isEmpty();

        long persistedTime = insertedAt;

        // Create and persist session state
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setRulebookHash("abc123");
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setPartialEvents(partialEvents);
        sessionState.setPersistedTime(persistedTime);
        sessionState.setCreatedTime(createdTime);
        sessionState.setCurrentStateSHA(calculateStateSHA(sessionState));

        stateManager.persistSessionState(sessionState);

        //--------
        long currentTime = rulesExecutor1.asKieSession().getSessionClock().getCurrentTime();

        // Simulate that a node crashes
        rulesExecutor1 = null;
        stateManager = null;

        // Recovery----
        // This test simulates that the restarted node recovers the session, assuming that the leader is taken over by another node
        HAStateManager stateManager2 = HAStateManagerFactory.create(TEST_DB_TYPE);
        stateManager2.initializeHA(HA_UUID, "worker-2", dbParams, dbHAConfig);

        SessionState retrievedSessionState = stateManager2.getPersistedSessionState("Test Ruleset");
        RulesExecutor rulesExecutorRecovered = stateManager2.recoverSession(ALL_CONDITION_RULE, retrievedSessionState, currentTime);

        rulesExecutorRecovered.advanceTime(10, java.util.concurrent.TimeUnit.SECONDS);

        String eventJson2 = "{\"j\":2}";

        matchList = rulesExecutorRecovered.processEvents(eventJson2).join();
        assertThat(matchList).hasSize(1);
    }

    public static final String ALL_CONDITION_WITH_FACT_RULE =
            """
            {
                "name": "Test Ruleset",
                "rules": [
                        {
                            "Rule": {
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Fact": "i"
                                                },
                                                "rhs": {
                                                    "Integer": 1
                                                }
                                            }
                                        },
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "j"
                                                },
                                                "rhs": {
                                                    "Integer": 2
                                                }
                                            }
                                        }
                                    ]
                                },
                                "enabled": true,
                                "name": null
                            }
                        }
                    ]
            }
            """;

    @Test
    void testFactRecordsReplayedOnRecovery() {
        stateManager.enableLeader();

        // This test works without HARulesExecutor
        RulesExecutor rulesExecutor1 = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK), ALL_CONDITION_WITH_FACT_RULE);
        long createdTime = rulesExecutor1.asKieSession().getSessionClock().getCurrentTime();

        long insertedAt = createdTime + 1_000;
        String factJson = "{\"i\":1}";

        List<Match> matchList = rulesExecutor1.processFacts(factJson).join(); // partial match
        assertThat(matchList).isEmpty();

        EventRecord factRecord = new EventRecord(factJson, insertedAt, EventRecord.RecordType.FACT);

        String rulebookHash = HAUtils.sha256(ALL_CONDITION_WITH_FACT_RULE);
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setRulebookHash(rulebookHash);
        sessionState.setPartialEvents(List.of(factRecord));
        sessionState.setCreatedTime(createdTime);
        sessionState.setPersistedTime(insertedAt);
        // Calculate SHA from complete state
        sessionState.setCurrentStateSHA(calculateStateSHA(sessionState));

        stateManager.persistSessionState(sessionState);
        stateManager.shutdown();
        stateManager = null;

        long currentTime = rulesExecutor1.asKieSession().getSessionClock().getCurrentTime();

        HAStateManager stateManager2 = HAStateManagerFactory.create(TEST_DB_TYPE);
        stateManager2.initializeHA(HA_UUID, "worker-2", dbParams, dbHAConfig);

        SessionState retrievedState = stateManager2.getPersistedSessionState("Test Ruleset");
        RulesExecutor recoveredExecutor = stateManager2.recoverSession(ALL_CONDITION_WITH_FACT_RULE, retrievedState, currentTime);

        String recoveredFacts = recoveredExecutor.getAllFactsAsJson();
        assertThat(recoveredFacts).contains("\"i\":1");

        stateManager2.shutdown();
    }

    @Test
    void testSessionStateShaFieldsPersistAndLoad() {
        stateManager.enableLeader();

        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setRulebookHash("rulebook-sha-001");
        sessionState.setPartialEvents(List.of());

        long now = System.currentTimeMillis();
        sessionState.setCreatedTime(now);
        sessionState.setPersistedTime(now);

        // Calculate SHA from complete state
        sessionState.setCurrentStateSHA(calculateStateSHA(sessionState));

        String expectedSha = sessionState.getCurrentStateSHA();

        stateManager.persistSessionState(sessionState);

        stateManager.shutdown();

        stateManager = HAStateManagerFactory.create(TEST_DB_TYPE);
        stateManager.initializeHA(HA_UUID, LEADER_ID, dbParams, dbHAConfig);

        SessionState retrieved = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(retrieved.getCurrentStateSHA()).isEqualTo(expectedSha);
        assertThat(retrieved.getRulebookHash()).isEqualTo("rulebook-sha-001");

        // Verify integrity by recalculating SHA
        String recalculatedSha = calculateStateSHA(retrieved);
        assertThat(retrieved.getCurrentStateSHA()).isEqualTo(recalculatedSha);
    }

    @Test
    void testPersistSessionStateRejectsMissingSha() {
        stateManager.enableLeader();

        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setRulebookHash("rulebook-sha-001");
        sessionState.setPartialEvents(List.of());

        long now = System.currentTimeMillis();
        sessionState.setCreatedTime(now);
        sessionState.setPersistedTime(now);

        assertThatThrownBy(() -> stateManager.persistSessionState(sessionState))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("currentStateSHA");
    }

    @Test
    void testPersistSessionStateRejectsMissingRulebookHash() {
        stateManager.enableLeader();

        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setPartialEvents(List.of());

        long now = System.currentTimeMillis();
        sessionState.setCreatedTime(now);
        sessionState.setPersistedTime(now);
        sessionState.setCurrentStateSHA(calculateStateSHA(sessionState));

        assertThatThrownBy(() -> stateManager.persistSessionState(sessionState))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rulebookHash");
    }

    @Test
    void testPersistSessionStateRejectsNonPositiveCreatedTime() {
        stateManager.enableLeader();

        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setRulebookHash("rulebook-sha-001");
        sessionState.setPartialEvents(List.of());
        sessionState.setCreatedTime(0L);
        sessionState.setPersistedTime(System.currentTimeMillis());
        sessionState.setCurrentStateSHA(calculateStateSHA(sessionState));

        assertThatThrownBy(() -> stateManager.persistSessionState(sessionState))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("createdTime");
    }

    @Test
    void testVerifySessionStateFailsFastOnTamperedSha() throws Exception {
        stateManager.enableLeader();

        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setRulebookHash("rulebook-sha-001");
        sessionState.setPartialEvents(List.of());
        long now = System.currentTimeMillis();
        sessionState.setCreatedTime(now);
        sessionState.setPersistedTime(now);
        sessionState.setCurrentStateSHA(calculateStateSHA(sessionState));
        stateManager.persistSessionState(sessionState);

        tamperSessionStateSha(HA_UUID, RULE_SET_NAME, "0000000000000000000000000000000000000000000000000000000000000000");

        SessionState tampered = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThatThrownBy(() -> stateManager.verifySessionState(tampered))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SessionState integrity check failed");
    }

    @Test
    void testVerifySessionStateFailsFastOnMissingSha() {
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setRulebookHash("rulebook-sha-001");
        long now = System.currentTimeMillis();
        sessionState.setCreatedTime(now);
        sessionState.setPersistedTime(now);

        assertThatThrownBy(() -> stateManager.verifySessionState(sessionState))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing SHA");
    }

    @Test
    void testMultipleRuleSetsPersistIndependently() {
        stateManager.enableLeader();

        SessionState rulesetA = new SessionState();
        rulesetA.setHaUuid(HA_UUID);
        rulesetA.setRuleSetName("rulesetA");
        rulesetA.setRulebookHash("hashA");
        rulesetA.setPartialEvents(List.of());
        rulesetA.setCurrentStateSHA(calculateStateSHA(rulesetA));
        stateManager.persistSessionState(rulesetA);

        SessionState rulesetB = new SessionState();
        rulesetB.setHaUuid(HA_UUID);
        rulesetB.setRuleSetName("rulesetB");
        rulesetB.setRulebookHash("hashB");
        rulesetB.setPartialEvents(List.of());
        rulesetB.setCurrentStateSHA(calculateStateSHA(rulesetB));
        stateManager.persistSessionState(rulesetB);

        SessionState retrievedA = stateManager.getPersistedSessionState("rulesetA");
        SessionState retrievedB = stateManager.getPersistedSessionState("rulesetB");

        assertThat(retrievedA.getRuleSetName()).isEqualTo("rulesetA");
        // SHA should match the calculated value from rulesetA
        assertThat(retrievedA.getCurrentStateSHA()).isEqualTo(calculateStateSHA(rulesetA));

        assertThat(retrievedB.getRuleSetName()).isEqualTo("rulesetB");
        // SHA should match the calculated value from rulesetB
        assertThat(retrievedB.getCurrentStateSHA()).isEqualTo(calculateStateSHA(rulesetB));
    }

    private void tamperSessionStateSha(String haUuid, String ruleSetName, String sha) throws Exception {
        String jdbcUrl;
        String user;
        String password;
        if ("postgres".equalsIgnoreCase((String) dbParams.get("db_type"))) {
            jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s",
                    dbParams.get("host"), dbParams.get("port"), dbParams.get("database"));
            user = (String) dbParams.get("user");
            password = (String) dbParams.get("password");
        } else {
            jdbcUrl = "jdbc:h2:file:" + dbParams.get("db_file_path") + ";MODE=PostgreSQL";
            user = "sa";
            password = "";
        }

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE drools_ansible_session_state SET current_state_sha = ? WHERE ha_uuid = ? AND rule_set_name = ?")) {
            ps.setString(1, sha);
            ps.setString(2, haUuid);
            ps.setString(3, ruleSetName);
            ps.executeUpdate();
        }
    }
}
