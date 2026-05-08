package org.drools.ansible.rulebook.integration.ha.tests;

/**
 * Base class for HAStateManager unit/component tests.
 * Supports both H2 and PostgreSQL based on system property 'test.db.type'.
 *
 * Usage:
 * - Default (H2): mvn test
 * - PostgreSQL: mvn test -Dtest.db.type=postgres
 */
abstract class HAStateManagerTestBase extends AbstractHATestBase {

    // Static initialization - runs once for all test classes
    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_unit_test", "HAStateManager tests");
        } else {
            initializeH2();
        }
    }
}
