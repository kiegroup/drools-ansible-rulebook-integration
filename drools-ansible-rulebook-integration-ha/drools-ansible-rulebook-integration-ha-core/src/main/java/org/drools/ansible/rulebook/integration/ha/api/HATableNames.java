package org.drools.ansible.rulebook.integration.ha.api;

/**
 * Constants for HA database table and index names.
 * All tables use the "drools_ansible_" prefix for easy identification and cleanup.
 */
public final class HATableNames {

    public static final String SESSION_STATE = "drools_ansible_session_state";
    public static final String MATCHING_EVENT = "drools_ansible_matching_event";
    public static final String EVENT_RECORD = "drools_ansible_event_record";
    public static final String ACTION_INFO = "drools_ansible_action_info";
    public static final String HA_STATS = "drools_ansible_ha_stats";
    public static final String IDX_MATCHING_EVENT_HA_UUID = "idx_da_matching_event_ha_uuid";
    public static final String IDX_EVENT_RECORD_ORDER = "idx_da_event_record_order";
    public static final String IDX_ACTION_INFO_ME = "idx_da_action_info_me";

    private HATableNames() {
    }
}
