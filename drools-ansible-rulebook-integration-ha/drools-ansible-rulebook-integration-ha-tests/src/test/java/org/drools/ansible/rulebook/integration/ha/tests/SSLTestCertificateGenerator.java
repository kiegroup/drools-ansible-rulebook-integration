package org.drools.ansible.rulebook.integration.ha.tests;

import java.io.FileWriter;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.bouncycastle.asn1.nist.NISTObjectIdentifiers;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OutputEncryptor;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfoBuilder;
import org.bouncycastle.pkcs.jcajce.JcePKCSPBEOutputEncryptorBuilder;

/**
 * Generates SSL certificates at test runtime for mTLS testing with PostgreSQL.
 * <p>
 * Produces:
 * <ul>
 *   <li>CA key pair + self-signed certificate (BasicConstraints CA:TRUE)</li>
 *   <li>Server key pair + certificate (SAN: DNS:localhost, IP:127.0.0.1; signed by CA)</li>
 *   <li>Client key pair + certificate (CN=test matching PostgreSQL role; signed by CA)</li>
 * </ul>
 */
final class SSLTestCertificateGenerator {

    static final String TEST_PASSPHRASE = "testpassphrase";

    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    private SSLTestCertificateGenerator() {
    }

    record CertBundle(
            Path caCert,
            Path serverKey,
            Path serverCert,
            Path clientKey,
            Path clientCert,
            String passphrase,
            Path baseDir,
            KeyPair clientKeyPair,
            KeyPair caKeyPair
    ) {}

    /**
     * Generate a full set of SSL certificates for testing.
     *
     * This is the basic bundle with client key in traditional PEM format encrypted with a passphrase (BEGIN RSA PRIVATE KEY + Proc-Type/DEK-Info).
     *
     * @param baseDir directory to write certificate files into
     * @return CertBundle with paths to all generated files
     */
    static CertBundle generate(Path baseDir) throws Exception {
        Files.createDirectories(baseDir);

        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048, new SecureRandom());

        // --- CA ---
        KeyPair caKeyPair = keyGen.generateKeyPair();
        X509Certificate caCert = buildCACertificate(caKeyPair);

        // --- Server ---
        KeyPair serverKeyPair = keyGen.generateKeyPair();
        X509Certificate serverCert = buildServerCertificate(serverKeyPair, caKeyPair, caCert);

        // --- Client ---
        KeyPair clientKeyPair = keyGen.generateKeyPair();
        X509Certificate clientCert = buildClientCertificate(clientKeyPair, caKeyPair, caCert);

        // Write files
        Path caCertPath = baseDir.resolve("ca.crt");
        Path serverKeyPath = baseDir.resolve("server.key");
        Path serverCertPath = baseDir.resolve("server.crt");
        Path clientKeyPath = baseDir.resolve("client.key");
        Path clientCertPath = baseDir.resolve("client.crt");

        writeCertPem(caCertPath, caCert);
        writeUnencryptedPkcs8Pem(serverKeyPath, serverKeyPair);
        writeCertPem(serverCertPath, serverCert);
        writeTraditionalEncryptedPem(clientKeyPath, clientKeyPair, TEST_PASSPHRASE.toCharArray());
        writeCertPem(clientCertPath, clientCert);

        return new CertBundle(caCertPath, serverKeyPath, serverCertPath, clientKeyPath, clientCertPath, TEST_PASSPHRASE, baseDir, clientKeyPair, caKeyPair);
    }

    private static X509Certificate buildCACertificate(KeyPair caKeyPair) throws Exception {
        X500Name issuer = new X500Name("CN=Test CA");
        Instant now = Instant.now();
        Date notBefore = Date.from(now);
        Date notAfter = Date.from(now.plus(Duration.ofDays(365)));

        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                issuer,
                BigInteger.valueOf(1),
                notBefore, notAfter,
                issuer,
                caKeyPair.getPublic()
        );
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(caKeyPair.getPrivate());
        X509CertificateHolder holder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(holder);
    }

    private static X509Certificate buildServerCertificate(KeyPair serverKeyPair, KeyPair caKeyPair, X509Certificate caCert) throws Exception {
        X500Name issuer = new X500Name("CN=Test CA");
        X500Name subject = new X500Name("CN=localhost");
        Instant now = Instant.now();
        Date notBefore = Date.from(now);
        Date notAfter = Date.from(now.plus(Duration.ofDays(365)));

        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                issuer,
                BigInteger.valueOf(2),
                notBefore, notAfter,
                subject,
                serverKeyPair.getPublic()
        );

        // SAN: DNS:localhost, IP:127.0.0.1
        GeneralNames san = new GeneralNames(new GeneralName[]{
                new GeneralName(GeneralName.dNSName, "localhost"),
                new GeneralName(GeneralName.iPAddress, "127.0.0.1")
        });
        builder.addExtension(Extension.subjectAlternativeName, false, san);
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(caKeyPair.getPrivate());
        X509CertificateHolder holder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(holder);
    }

    private static X509Certificate buildClientCertificate(KeyPair clientKeyPair, KeyPair caKeyPair, X509Certificate caCert) throws Exception {
        return buildClientCertificate(clientKeyPair, caKeyPair, caCert, "test");
    }

    private static void writeCertPem(Path path, X509Certificate cert) throws Exception {
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(path.toFile()))) {
            writer.writeObject(cert);
        }
    }

    // ---- Bundle variant factories ----
    // Each method writes the client key in a specific format and returns a new CertBundle
    // with clientKey pointing to that file. CA, server, and client cert are shared from base.

    /**
     * Derive a bundle with client key as unencrypted PKCS#8 PEM: {@code -----BEGIN PRIVATE KEY-----}
     * <p>Most common format in Ansible/OpenShift deployments.
     */
    static CertBundle withUnencryptedPkcs8PemKey(CertBundle base, Path outputPath) throws Exception {
        writeUnencryptedPkcs8Pem(outputPath, base.clientKeyPair());
        return replaceClientKey(base, outputPath, null);
    }

    /**
     * Derive a bundle with client key as unencrypted PKCS#1 PEM: {@code -----BEGIN RSA PRIVATE KEY-----}
     * <p>Traditional OpenSSL RSA key format without encryption.
     */
    static CertBundle withUnencryptedPkcs1PemKey(CertBundle base, Path outputPath) throws Exception {
        writeUnencryptedPkcs1Pem(outputPath, base.clientKeyPair());
        return replaceClientKey(base, outputPath, null);
    }

    /**
     * Derive a bundle with client key as encrypted PKCS#8 PEM (PBES2): {@code -----BEGIN ENCRYPTED PRIVATE KEY-----}
     */
    static CertBundle withPkcs8EncryptedPemKey(CertBundle base, Path outputPath, String passphrase) throws Exception {
        writePkcs8EncryptedPem(outputPath, base.clientKeyPair(), passphrase.toCharArray());
        return replaceClientKey(base, outputPath, passphrase);
    }

    /**
     * Derive a bundle with client key + cert as a PKCS#12 keystore with password.
     * <p>Output path must end with {@code .p12} for format detection by {@code PostgreSQLStateManager}.
     */
    static CertBundle withPkcs12Key(CertBundle base, Path outputPath, String passphrase) throws Exception {
        writePkcs12(outputPath, base.clientKeyPair(), base.clientCert(), passphrase.toCharArray());
        return replaceClientKey(base, outputPath, passphrase);
    }

    /**
     * Derive a bundle with client key + cert as a PKCS#12 keystore without password.
     * <p>Uses empty password internally because pgjdbc's {@code PKCS12KeyManager} requires
     * an {@code sslpassword} parameter (falls back to console callback otherwise).
     * <p>Output path must end with {@code .p12} for format detection by {@code PostgreSQLStateManager}.
     */
    static CertBundle withPkcs12KeyNoPassword(CertBundle base, Path outputPath) throws Exception {
        writePkcs12(outputPath, base.clientKeyPair(), base.clientCert(), new char[0]);
        return replaceClientKey(base, outputPath, "");
    }

    /**
     * Derive a bundle with client key as encrypted PKCS#8 DER (PBES2, PBKDF2-HMAC-SHA256, AES-256-CBC).
     * <p>Output path should end with {@code .der} or {@code .pk8} for format detection by {@code PostgreSQLStateManager}.
     */
    static CertBundle withDerEncryptedKey(CertBundle base, Path outputPath, String passphrase) throws Exception {
        writeDerEncryptedPkcs8(outputPath, base.clientKeyPair(), passphrase.toCharArray());
        return replaceClientKey(base, outputPath, passphrase);
    }

    /**
     * Derive a bundle with client key as unencrypted PKCS#8 DER (raw binary).
     */
    static CertBundle withDerUnencryptedKey(CertBundle base, Path outputPath) throws Exception {
        Files.write(outputPath, base.clientKeyPair().getPrivate().getEncoded());
        return replaceClientKey(base, outputPath, null);
    }

    // ---- Internal key format writers ----

    private static void writeUnencryptedPkcs1Pem(Path path, KeyPair keyPair) throws Exception {
        // JcaPEMWriter.writeObject(privateKey) writes PKCS#1 (BEGIN RSA PRIVATE KEY) for RSA keys
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(path.toFile()))) {
            writer.writeObject(keyPair.getPrivate());
        }
    }

    private static void writeUnencryptedPkcs8Pem(Path path, KeyPair keyPair) throws Exception {
        // Use JcaPKCS8Generator to produce PKCS#8 PEM (BEGIN PRIVATE KEY)
        // JcaPEMWriter.writeObject(privateKey) writes PKCS#1 (BEGIN RSA PRIVATE KEY) for RSA keys
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(path.toFile()))) {
            writer.writeObject(new JcaPKCS8Generator(keyPair.getPrivate(), null));
        }
    }

    private static void writeDerEncryptedPkcs8(Path path, KeyPair keyPair, char[] passphrase) throws Exception {
        // PBES2 with PBKDF2-HMAC-SHA256 + AES-256-CBC
        PrivateKeyInfo keyInfo = PrivateKeyInfo.getInstance(keyPair.getPrivate().getEncoded());
        PKCS8EncryptedPrivateKeyInfoBuilder builder = new PKCS8EncryptedPrivateKeyInfoBuilder(keyInfo);
        OutputEncryptor encryptor = new JcePKCSPBEOutputEncryptorBuilder(NISTObjectIdentifiers.id_aes256_CBC)
                .setProvider("BC")
                .build(passphrase);
        Files.write(path, builder.build(encryptor).getEncoded());
    }

    private static void writePkcs8EncryptedPem(Path path, KeyPair keyPair, char[] passphrase) throws Exception {
        // PBES2 with AES-256-CBC produces "BEGIN ENCRYPTED PRIVATE KEY"
        JceOpenSSLPKCS8EncryptorBuilder encryptorBuilder =
                new JceOpenSSLPKCS8EncryptorBuilder(NISTObjectIdentifiers.id_aes256_CBC);
        encryptorBuilder.setProvider("BC");
        encryptorBuilder.setPassword(passphrase);
        OutputEncryptor encryptor = encryptorBuilder.build();
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(path.toFile()))) {
            writer.writeObject(new JcaPKCS8Generator(keyPair.getPrivate(), encryptor));
        }
    }

    private static void writePkcs12(Path path, KeyPair keyPair, Path clientCertPath, char[] passphrase) throws Exception {
        // Load client certificate from PEM file
        java.security.cert.CertificateFactory cf = java.security.cert.CertificateFactory.getInstance("X.509");
        Certificate cert;
        try (var is = Files.newInputStream(clientCertPath)) {
            cert = cf.generateCertificate(is);
        }

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, passphrase);
        ks.setKeyEntry("user", keyPair.getPrivate(), passphrase, new Certificate[]{cert});
        try (OutputStream os = Files.newOutputStream(path)) {
            ks.store(os, passphrase);
        }
    }

    private static void writeTraditionalEncryptedPem(Path path, KeyPair keyPair, char[] passphrase) throws Exception {
        JcePEMEncryptorBuilder encBuilder = new JcePEMEncryptorBuilder("AES-256-CBC");
        encBuilder.setProvider("BC");
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(path.toFile()))) {
            writer.writeObject(keyPair.getPrivate(), encBuilder.build(passphrase));
        }
    }

    private static CertBundle replaceClientKey(CertBundle base, Path newClientKey, String newPassphrase) {
        return new CertBundle(
                base.caCert(), base.serverKey(), base.serverCert(),
                newClientKey, base.clientCert(),
                newPassphrase,
                base.baseDir(), base.clientKeyPair(), base.caKeyPair());
    }

    /**
     * Derive a bundle with a new client certificate using a different CN.
     * The new client cert is signed by the same CA as the original bundle.
     * The client key is in traditional encrypted PEM format (same as default {@link #generate}).
     *
     * @param base      the original bundle (provides CA key pair for signing)
     * @param cn        the CN for the new client certificate (e.g., "wronguser")
     * @param outputDir directory to write the new client key and cert files
     * @return a new CertBundle with the replaced client key/cert
     */
    static CertBundle withClientCN(CertBundle base, String cn, Path outputDir) throws Exception {
        Files.createDirectories(outputDir);

        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048, new SecureRandom());
        KeyPair newClientKeyPair = keyGen.generateKeyPair();

        // Load the CA certificate from the base bundle
        java.security.cert.CertificateFactory cf = java.security.cert.CertificateFactory.getInstance("X.509");
        X509Certificate caCert;
        try (var is = Files.newInputStream(base.caCert())) {
            caCert = (X509Certificate) cf.generateCertificate(is);
        }

        X509Certificate newClientCert = buildClientCertificate(newClientKeyPair, base.caKeyPair(), caCert, cn);

        Path newClientKeyPath = outputDir.resolve("client.key");
        Path newClientCertPath = outputDir.resolve("client.crt");

        writeTraditionalEncryptedPem(newClientKeyPath, newClientKeyPair, TEST_PASSPHRASE.toCharArray());
        writeCertPem(newClientCertPath, newClientCert);

        return new CertBundle(
                base.caCert(), base.serverKey(), base.serverCert(),
                newClientKeyPath, newClientCertPath,
                TEST_PASSPHRASE,
                outputDir, newClientKeyPair, base.caKeyPair());
    }

    private static X509Certificate buildClientCertificate(KeyPair clientKeyPair, KeyPair caKeyPair,
                                                          X509Certificate caCert, String cn) throws Exception {
        X500Name issuer = new X500Name("CN=Test CA");
        X500Name subject = new X500Name("CN=" + cn);
        Instant now = Instant.now();
        Date notBefore = Date.from(now);
        Date notAfter = Date.from(now.plus(Duration.ofDays(365)));

        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                issuer,
                BigInteger.valueOf(System.nanoTime()),
                notBefore, notAfter,
                subject,
                clientKeyPair.getPublic()
        );
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.digitalSignature));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(caKeyPair.getPrivate());
        X509CertificateHolder holder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(holder);
    }
}
