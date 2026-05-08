package org.drools.ansible.rulebook.integration.ha.tests;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

import org.drools.ansible.rulebook.integration.ha.postgres.PemToKeyStoreConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for PEM-to-PKCS#12 key conversion.
 * <p>
 * For SSL integration tests against a real PostgreSQL container, see {@link PostgresSSLJdbcTest}.
 */
class PemToKeyStoreConverterTest {

    @TempDir
    Path tempDir;

    // Key format: Traditional OpenSSL PEM encryption (Proc-Type / DEK-Info)
    @Test
    void testConvertTraditionalEncryptedPemToP12() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        Path p12Path = PemToKeyStoreConverter.convertPemToP12(
                bundle.clientKey().toString(),
                bundle.clientCert().toString(),
                bundle.passphrase().toCharArray());

        assertThat(p12Path).exists();
        assertThat(p12Path.toString()).endsWith(".p12");

        // Load and verify the PKCS#12 keystore
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var is = Files.newInputStream(p12Path)) {
            ks.load(is, bundle.passphrase().toCharArray());
        }

        Enumeration<String> aliases = ks.aliases();
        assertThat(aliases.hasMoreElements()).isTrue();

        String alias = aliases.nextElement();
        assertThat(ks.isKeyEntry(alias)).isTrue();

        PrivateKey key = (PrivateKey) ks.getKey(alias, bundle.passphrase().toCharArray());
        assertThat(key).isNotNull();
        assertThat(key.getAlgorithm()).isEqualTo("RSA");

        Certificate[] chain = ks.getCertificateChain(alias);
        assertThat(chain).isNotNull().hasSize(1);
        X509Certificate cert = (X509Certificate) chain[0];
        assertThat(cert.getSubjectX500Principal().getName()).contains("CN=test");

        PemToKeyStoreConverter.cleanup(p12Path);
        assertThat(p12Path).doesNotExist();
    }

    // Key format: unencrypted PEM PKCS#1
    @Test
    void testConvertUnencryptedPkcs1PemToP12() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs-pkcs1"));

        // Write client key as unencrypted PKCS#1 PEM (BEGIN RSA PRIVATE KEY)
        SSLTestCertificateGenerator.CertBundle pkcs1Bundle =
                SSLTestCertificateGenerator.withUnencryptedPkcs1PemKey(bundle, tempDir.resolve("client-pkcs1.pem"));

        // Verify the PEM file has the expected PKCS#1 header
        String pemContent = Files.readString(pkcs1Bundle.clientKey());
        assertThat(pemContent).contains("-----BEGIN RSA PRIVATE KEY-----");
        assertThat(pemContent).doesNotContain("ENCRYPTED");
        assertThat(pemContent).doesNotContain("BEGIN PRIVATE KEY"); // not PKCS#8

        // Convert to P12 with a generated passphrase (no key passphrase needed for unencrypted)
        Path p12Path = PemToKeyStoreConverter.convertPemToP12(
                pkcs1Bundle.clientKey().toString(),
                pkcs1Bundle.clientCert().toString(),
                "generated-passphrase".toCharArray());

        try {
            assertThat(p12Path).exists();
            assertThat(p12Path.toString()).endsWith(".p12");

            // Verify the P12 keystore contains the correct key and certificate
            KeyStore ks = KeyStore.getInstance("PKCS12");
            try (var is = Files.newInputStream(p12Path)) {
                ks.load(is, "generated-passphrase".toCharArray());
            }

            String alias = ks.aliases().nextElement();
            assertThat(ks.isKeyEntry(alias)).isTrue();

            PrivateKey key = (PrivateKey) ks.getKey(alias, "generated-passphrase".toCharArray());
            assertThat(key).isNotNull();
            assertThat(key.getAlgorithm()).isEqualTo("RSA");

            Certificate[] chain = ks.getCertificateChain(alias);
            assertThat(chain).isNotNull().hasSize(1);
            X509Certificate cert = (X509Certificate) chain[0];
            assertThat(cert.getSubjectX500Principal().getName()).contains("CN=test");
        } finally {
            PemToKeyStoreConverter.cleanup(p12Path);
        }
    }

    // Key format: PKCS#8 encrypted PEM (PBES2)
    @Test
    void testConvertPkcs8EncryptedPemToP12() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs-pkcs8enc"));

        // Write client key as encrypted PKCS#8 PEM (BEGIN ENCRYPTED PRIVATE KEY)
        SSLTestCertificateGenerator.CertBundle encBundle =
                SSLTestCertificateGenerator.withPkcs8EncryptedPemKey(bundle, tempDir.resolve("client-pkcs8-enc.pem"),
                        SSLTestCertificateGenerator.TEST_PASSPHRASE);

        // Verify the PEM file has the expected PKCS#8 encrypted header
        String pemContent = Files.readString(encBundle.clientKey());
        assertThat(pemContent).contains("-----BEGIN ENCRYPTED PRIVATE KEY-----");

        // Convert to P12 using the passphrase
        Path p12Path = PemToKeyStoreConverter.convertPemToP12(
                encBundle.clientKey().toString(),
                encBundle.clientCert().toString(),
                encBundle.passphrase().toCharArray());

        try {
            assertThat(p12Path).exists();
            assertThat(p12Path.toString()).endsWith(".p12");

            // Verify the P12 keystore contains the correct key and certificate
            KeyStore ks = KeyStore.getInstance("PKCS12");
            try (var is = Files.newInputStream(p12Path)) {
                ks.load(is, encBundle.passphrase().toCharArray());
            }

            String alias = ks.aliases().nextElement();
            assertThat(ks.isKeyEntry(alias)).isTrue();

            PrivateKey key = (PrivateKey) ks.getKey(alias, encBundle.passphrase().toCharArray());
            assertThat(key).isNotNull();
            assertThat(key.getAlgorithm()).isEqualTo("RSA");

            Certificate[] chain = ks.getCertificateChain(alias);
            assertThat(chain).isNotNull().hasSize(1);
            X509Certificate cert = (X509Certificate) chain[0];
            assertThat(cert.getSubjectX500Principal().getName()).contains("CN=test");
        } finally {
            PemToKeyStoreConverter.cleanup(p12Path);
        }
    }

    // Key format: unencrypted PEM PKCS#8
    @Test
    void testConvertUnencryptedPkcs8PemToP12() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        // Write client key as unencrypted PKCS#8 PEM (BEGIN PRIVATE KEY)
        SSLTestCertificateGenerator.CertBundle pkcs8Bundle =
                SSLTestCertificateGenerator.withUnencryptedPkcs8PemKey(bundle, tempDir.resolve("client-pkcs8.pem"));

        // Verify the PEM file has the expected header
        String pemContent = Files.readString(pkcs8Bundle.clientKey());
        assertThat(pemContent).contains("-----BEGIN PRIVATE KEY-----");
        assertThat(pemContent).doesNotContain("ENCRYPTED");

        // Convert to P12 with a generated passphrase (simulates what PostgreSQLStateManager will do)
        Path p12Path = PemToKeyStoreConverter.convertPemToP12(
                pkcs8Bundle.clientKey().toString(),
                pkcs8Bundle.clientCert().toString(),
                "generated-passphrase".toCharArray());

        assertThat(p12Path).exists();
        assertThat(p12Path.toString()).endsWith(".p12");

        // Verify the P12 keystore contains the correct key and certificate
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var is = Files.newInputStream(p12Path)) {
            ks.load(is, "generated-passphrase".toCharArray());
        }

        String alias = ks.aliases().nextElement();
        assertThat(ks.isKeyEntry(alias)).isTrue();

        PrivateKey key = (PrivateKey) ks.getKey(alias, "generated-passphrase".toCharArray());
        assertThat(key).isNotNull();
        assertThat(key.getAlgorithm()).isEqualTo("RSA");

        Certificate[] chain = ks.getCertificateChain(alias);
        assertThat(chain).isNotNull().hasSize(1);
        X509Certificate cert = (X509Certificate) chain[0];
        assertThat(cert.getSubjectX500Principal().getName()).contains("CN=test");

        PemToKeyStoreConverter.cleanup(p12Path);
    }

    // --- Negative tests ---

    @Test
    void testCorruptedPemFile() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs-corrupt"));

        // Write garbage data as a PEM key file
        Path corruptKey = tempDir.resolve("corrupt.pem");
        Files.writeString(corruptKey, "not a valid PEM file at all\ngarbage data\n");

        assertThatThrownBy(() -> PemToKeyStoreConverter.convertPemToP12(
                corruptKey.toString(),
                bundle.clientCert().toString(),
                "passphrase".toCharArray()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to convert PEM to PKCS#12 keystore");
    }

    @Test
    void testWrongPassphrase() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs-wrongpass"));

        // The default bundle has a traditional encrypted PEM key with TEST_PASSPHRASE
        assertThatThrownBy(() -> PemToKeyStoreConverter.convertPemToP12(
                bundle.clientKey().toString(),
                bundle.clientCert().toString(),
                "wrong-passphrase".toCharArray()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to convert PEM to PKCS#12 keystore");
    }

    @Test
    void testEmptyCertificateFile() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs-emptycert"));

        // Create an empty certificate file
        Path emptyCert = tempDir.resolve("empty.crt");
        Files.writeString(emptyCert, "");

        assertThatThrownBy(() -> PemToKeyStoreConverter.convertPemToP12(
                bundle.clientKey().toString(),
                emptyCert.toString(),
                bundle.passphrase().toCharArray()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to convert PEM to PKCS#12 keystore")
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No certificates found in");
    }

    @Test
    void testUnsupportedPemObjectType() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs-unsupported"));

        // Pass the certificate file as the key file — PEMParser will parse it as
        // X509CertificateHolder, which is not a supported key type
        assertThatThrownBy(() -> PemToKeyStoreConverter.convertPemToP12(
                bundle.clientCert().toString(),
                bundle.clientCert().toString(),
                "passphrase".toCharArray()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to convert PEM to PKCS#12 keystore")
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported PEM object type");
    }
}
