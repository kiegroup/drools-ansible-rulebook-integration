package org.drools.ansible.rulebook.integration.ha.postgres;

import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import com.zaxxer.hikari.pool.HikariPool;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import static org.assertj.core.api.Assertions.assertThat;

class PostgreSQLConnectionFailureMessageTest {

    @Test
    void formatsTimeoutFailureWithCredentialContextAndRootCause() {
        SocketTimeoutException timeout = new SocketTimeoutException("Connect timed out");
        PSQLException driverFailure = new PSQLException("The connection attempt failed.", PSQLState.CONNECTION_UNABLE_TO_CONNECT, timeout);
        HikariPool.PoolInitializationException poolFailure =
                new HikariPool.PoolInitializationException(driverFailure);

        String message = PostgreSQLStateManager.formatConnectionFailureMessage(
                "54.164.199.254",
                "5432",
                "eda_ha",
                "require",
                poolFailure);

        System.out.println(message);

        assertThat(message).contains("host=54.164.199.254");
        assertThat(message).contains("port=5432");
        assertThat(message).contains("database=eda_ha");
        assertThat(message).contains("sslmode=require");
        assertThat(message).contains("SocketTimeoutException: Connect timed out");
        assertThat(message).contains("Verify connection parameters and network reachability.");
    }

    @Test
    void formatsUnknownHostFailureWithoutLeakingSensitiveValues() {
        UnknownHostException unknownHost = new UnknownHostException("db.internal.example");

        String message = PostgreSQLStateManager.formatConnectionFailureMessage(
                "db.internal.example",
                "5432",
                "eda_ha",
                "verify-full",
                unknownHost);

        assertThat(message).contains("UnknownHostException: db.internal.example");
        assertThat(message).doesNotContain("password");
        assertThat(message).doesNotContain("secret");
    }
}
