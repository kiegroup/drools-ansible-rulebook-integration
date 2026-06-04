package org.drools.ansible.rulebook.integration.ha.postgres;

import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import java.security.Security;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts PEM private key + certificate files into a PKCS#12 keystore.
 * <p>
 * The PostgreSQL JDBC driver (pgjdbc) cannot handle encrypted PEM PKCS#8 keys directly.
 * This utility converts PEM files into a PKCS#12 (.p12) keystore that pgjdbc can use
 * via its {@code sslkey} parameter.
 */
public final class PemToKeyStoreConverter {

    private static final Logger logger = LoggerFactory.getLogger(PemToKeyStoreConverter.class);

    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    private PemToKeyStoreConverter() {
    }

    /**
     * Convert a PEM private key and certificate to a PKCS#12 keystore file.
     *
     * @param pemKeyPath  path to the PEM private key file (encrypted or unencrypted PKCS#8, or traditional format)
     * @param pemCertPath path to the PEM client certificate file
     * @param passphrase  passphrase for the encrypted key (also used as the PKCS#12 keystore password)
     * @return path to the temporary .p12 keystore file
     */
    public static Path convertPemToP12(String pemKeyPath, String pemCertPath, char[] passphrase) {
        try {
            PrivateKey privateKey = readPrivateKey(pemKeyPath, passphrase);
            Certificate[] certChain = readCertificateChain(pemCertPath);
            return buildP12Keystore(privateKey, certChain, passphrase);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert PEM to PKCS#12 keystore", e);
        }
    }

    /**
     * Convert an encrypted PKCS#8 DER private key and certificate to a PKCS#12 keystore file.
     * <p>
     * pgjdbc's {@code LazyKeyManager} cannot decrypt PBES2-encrypted DER keys because the
     * standard JDK does not support "PBES2" as a cipher name. This method uses BouncyCastle
     * to decrypt the key and package it into a PKCS#12 keystore that pgjdbc can use.
     *
     * @param derKeyPath  path to the encrypted PKCS#8 DER private key file
     * @param pemCertPath path to the PEM client certificate file
     * @param passphrase  passphrase for the encrypted key (also used as the PKCS#12 keystore password)
     * @return path to the temporary .p12 keystore file
     */
    public static Path convertDerToP12(String derKeyPath, String pemCertPath, char[] passphrase) {
        try {
            PrivateKey privateKey = readDerPrivateKey(derKeyPath, passphrase);
            Certificate[] certChain = readCertificateChain(pemCertPath);
            return buildP12Keystore(privateKey, certChain, passphrase);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert encrypted DER to PKCS#12 keystore", e);
        }
    }

    private static Path buildP12Keystore(PrivateKey privateKey, Certificate[] certChain, char[] passphrase) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, passphrase);
        keyStore.setKeyEntry("user", privateKey, passphrase, certChain);

        Path tempDir = Files.createTempDirectory("drools-ha-ssl-");
        Path p12Path = tempDir.resolve("client-keystore.p12");
        try (OutputStream os = Files.newOutputStream(p12Path)) {
            keyStore.store(os, passphrase);
        }

        // Safety net: delete on JVM exit
        p12Path.toFile().deleteOnExit();
        tempDir.toFile().deleteOnExit();

        logger.info("Created temporary PKCS#12 keystore at {}", redactPathForInfoLog(p12Path));
        logger.debug("  --- keystore path: {}", p12Path);
        return p12Path;
    }

    /**
     * Clean up the temporary PKCS#12 keystore file and its parent directory.
     */
    public static void cleanup(Path p12Path) {
        if (p12Path == null) {
            return;
        }
        try {
            Files.deleteIfExists(p12Path);
            Path parentDir = p12Path.getParent();
            if (parentDir != null) {
                Files.deleteIfExists(parentDir);
            }
            logger.info("Cleaned up temporary PKCS#12 keystore at {}", redactPathForInfoLog(p12Path));
            logger.debug("  --- keystore path: {}", p12Path);
        } catch (IOException e) {
            logger.warn("Failed to clean up temporary PKCS#12 keystore at {}: {}",
                    redactPathForInfoLog(p12Path), e.getMessage());
            logger.debug("  --- keystore path: {}", p12Path, e);
        }
    }

    private static String redactPathForInfoLog(Path path) {
        if (path == null) {
            return "unknown";
        }
        Path fileName = path.getFileName();
        if (fileName == null) {
            return path.toString();
        }
        Path parent = path.getParent();
        if (parent == null) {
            return fileName.toString();
        }
        Path parentName = parent.getFileName();
        if (parentName == null) {
            return fileName.toString();
        }
        return ".../" + parentName + "/" + fileName;
    }

    private static PrivateKey readDerPrivateKey(String derKeyPath, char[] passphrase) throws Exception {
        byte[] derBytes = Files.readAllBytes(Path.of(derKeyPath));
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter();

        PKCS8EncryptedPrivateKeyInfo encryptedInfo = new PKCS8EncryptedPrivateKeyInfo(derBytes);
        InputDecryptorProvider decryptor = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase);
        PrivateKeyInfo keyInfo = encryptedInfo.decryptPrivateKeyInfo(decryptor);
        return converter.getPrivateKey(keyInfo);
    }

    private static PrivateKey readPrivateKey(String pemKeyPath, char[] passphrase) throws Exception {
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter();

        try (PEMParser parser = new PEMParser(new FileReader(pemKeyPath))) {
            Object object = parser.readObject();

            if (object instanceof PKCS8EncryptedPrivateKeyInfo encryptedInfo) {
                // Encrypted PKCS#8 PEM (e.g., "-----BEGIN ENCRYPTED PRIVATE KEY-----")
                InputDecryptorProvider decryptor = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase);
                PrivateKeyInfo keyInfo = encryptedInfo.decryptPrivateKeyInfo(decryptor);
                return converter.getPrivateKey(keyInfo);
            } else if (object instanceof PrivateKeyInfo keyInfo) {
                // Unencrypted PKCS#8 PEM (e.g., "-----BEGIN PRIVATE KEY-----")
                return converter.getPrivateKey(keyInfo);
            } else if (object instanceof PEMEncryptedKeyPair encryptedKeyPair) {
                // Traditional encrypted PEM (e.g., "-----BEGIN RSA PRIVATE KEY-----" with DEK-Info)
                PEMKeyPair keyPair = encryptedKeyPair.decryptKeyPair(
                        new JcePEMDecryptorProviderBuilder().build(passphrase));
                return converter.getPrivateKey(keyPair.getPrivateKeyInfo());
            } else if (object instanceof PEMKeyPair keyPair) {
                // Traditional unencrypted PEM
                return converter.getPrivateKey(keyPair.getPrivateKeyInfo());
            } else {
                throw new IllegalArgumentException("Unsupported PEM object type: " +
                        (object != null ? object.getClass().getName() : "null"));
            }
        }
    }

    private static Certificate[] readCertificateChain(String pemCertPath) throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Collection<? extends Certificate> certs;
        try (var is = Files.newInputStream(Path.of(pemCertPath))) {
            certs = cf.generateCertificates(is);
        }

        if (certs.isEmpty()) {
            throw new IllegalArgumentException("No certificates found in " + pemCertPath);
        }

        List<X509Certificate> certList = new ArrayList<>();
        for (Certificate cert : certs) {
            certList.add((X509Certificate) cert);
        }
        return certList.toArray(new Certificate[0]);
    }
}
