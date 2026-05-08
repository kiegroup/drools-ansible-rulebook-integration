package org.drools.ansible.rulebook.integration.ha.tests;

import org.drools.ansible.rulebook.integration.ha.postgres.PostgreSQLStateManager;
import org.drools.ansible.rulebook.integration.ha.postgres.PostgreSQLStateManager.SslKeyFormat;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PostgreSQLStateManager#detectSslKeyFormat(String)}.
 * <p>
 * This method dispatches SSL key handling based on file extension.
 */
class DetectSslKeyFormatTest {

    @ParameterizedTest(name = "[{index}] \"{0}\" -> {1}")
    @CsvSource({
            "client.p12,        PKCS12",
            "client.pfx,        PKCS12",
            "client.P12,        PKCS12",
            "client.PFX,        PKCS12",
            "client.pk8,        DER",
            "client.der,        DER",
            "client.PK8,        DER",
            "client.DER,        DER",
            "client.pem,        PEM",
            "client.key,        PEM",
            "/tmp/v2.0/keyfile,  PEM",
    })
    void testDetectSslKeyFormat(String path, SslKeyFormat expectedFormat) {
        assertThat(PostgreSQLStateManager.detectSslKeyFormat(path)).isEqualTo(expectedFormat);
    }
}
