package org.drools.ansible.rulebook.integration.ha.tests.ssl;

import java.util.Base64;

import javax.crypto.KeyGenerator;

import org.drools.ansible.rulebook.integration.ha.api.HAEncryption;
import org.drools.ansible.rulebook.integration.ha.api.HAEncryptionException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for HAEncryption (AES-256-GCM encrypt/decrypt with primary/secondary key).
 */
class HAEncryptionTest {

    private static String generateBase64Key() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256);
            return Base64.getEncoder().encodeToString(keyGen.generateKey().getEncoded());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void encryptDecryptRoundTrip() {
        String key = generateBase64Key();
        HAEncryption encryption = new HAEncryption(key, null);

        String plaintext = "{\"temperature\": 35, \"host\": \"server-01\"}";
        String encrypted = encryption.encrypt(plaintext);
        HAEncryption.DecryptResult result = encryption.decrypt(encrypted);

        assertThat(result.plaintext()).isEqualTo(plaintext);
        assertThat(result.usedSecondaryKey()).isFalse();
    }

    @Test
    void encryptProducesPrefix() {
        String key = generateBase64Key();
        HAEncryption encryption = new HAEncryption(key, null);

        String encrypted = encryption.encrypt("test data");
        assertThat(encrypted).startsWith("$ENCRYPTED$");
    }

    @Test
    void decryptPlaintextPassthrough() {
        String key = generateBase64Key();
        HAEncryption encryption = new HAEncryption(key, null);

        String plaintext = "{\"temperature\": 35}";
        HAEncryption.DecryptResult result = encryption.decrypt(plaintext);

        assertThat(result.plaintext()).isEqualTo(plaintext);
        assertThat(result.usedSecondaryKey()).isFalse();
    }

    @Test
    void decryptWithSecondaryKeyFallback() {
        String keyA = generateBase64Key();
        String keyB = generateBase64Key();

        // Encrypt with keyA
        HAEncryption encryptorA = new HAEncryption(keyA, null);
        String encrypted = encryptorA.encrypt("secret data");

        // Decrypt with (keyB as primary, keyA as secondary) — should fall back to secondary
        HAEncryption decryptor = new HAEncryption(keyB, keyA);
        HAEncryption.DecryptResult result = decryptor.decrypt(encrypted);

        assertThat(result.plaintext()).isEqualTo("secret data");
        assertThat(result.usedSecondaryKey()).isTrue();
    }

    @Test
    void decryptWithPrimaryKeyDirectly() {
        String keyA = generateBase64Key();
        String keyB = generateBase64Key();

        // Encrypt with keyB
        HAEncryption encryptorB = new HAEncryption(keyB, null);
        String encrypted = encryptorB.encrypt("secret data");

        // Decrypt with (keyB as primary, keyA as secondary) — should use primary
        HAEncryption decryptor = new HAEncryption(keyB, keyA);
        HAEncryption.DecryptResult result = decryptor.decrypt(encrypted);

        assertThat(result.plaintext()).isEqualTo("secret data");
        assertThat(result.usedSecondaryKey()).isFalse();
    }

    @Test
    void bothKeysFail_fatalError() {
        String keyA = generateBase64Key();
        String keyB = generateBase64Key();
        String keyC = generateBase64Key();

        // Encrypt with keyA
        HAEncryption encryptorA = new HAEncryption(keyA, null);
        String encrypted = encryptorA.encrypt("secret");

        // Decrypt with (keyB, keyC) — both wrong
        HAEncryption decryptor = new HAEncryption(keyB, keyC);
        assertThatThrownBy(() -> decryptor.decrypt(encrypted))
                .isInstanceOf(HAEncryptionException.class)
                .hasMessageContaining("FATAL")
                .hasMessageContaining("both primary and secondary keys");
    }

    @Test
    void noSecondaryKey_primaryFails_fatalError() {
        String keyA = generateBase64Key();
        String keyB = generateBase64Key();

        // Encrypt with keyA
        HAEncryption encryptorA = new HAEncryption(keyA, null);
        String encrypted = encryptorA.encrypt("secret");

        // Decrypt with keyB only (no secondary)
        HAEncryption decryptor = new HAEncryption(keyB, null);
        assertThatThrownBy(() -> decryptor.decrypt(encrypted))
                .isInstanceOf(HAEncryptionException.class)
                .hasMessageContaining("FATAL");
    }

    @Test
    void decryptResultUsedSecondaryKey() {
        String keyA = generateBase64Key();
        String keyB = generateBase64Key();

        HAEncryption encryptorA = new HAEncryption(keyA, null);
        String encrypted = encryptorA.encrypt("data");

        // Primary key match — usedSecondaryKey should be false
        HAEncryption decryptorPrimary = new HAEncryption(keyA, keyB);
        assertThat(decryptorPrimary.decrypt(encrypted).usedSecondaryKey()).isFalse();

        // Secondary key match — usedSecondaryKey should be true
        HAEncryption decryptorSecondary = new HAEncryption(keyB, keyA);
        assertThat(decryptorSecondary.decrypt(encrypted).usedSecondaryKey()).isTrue();
    }

    @Test
    void ivUniqueness() {
        String key = generateBase64Key();
        HAEncryption encryption = new HAEncryption(key, null);

        String plaintext = "same data";
        String encrypted1 = encryption.encrypt(plaintext);
        String encrypted2 = encryption.encrypt(plaintext);

        // Same plaintext should produce different ciphertext (random IV)
        assertThat(encrypted1).isNotEqualTo(encrypted2);

        // Both should decrypt to the same plaintext
        assertThat(encryption.decrypt(encrypted1).plaintext()).isEqualTo(plaintext);
        assertThat(encryption.decrypt(encrypted2).plaintext()).isEqualTo(plaintext);
    }

    @Test
    void nullAndEmptyHandling() {
        String key = generateBase64Key();
        HAEncryption encryption = new HAEncryption(key, null);

        // encrypt(null) returns null
        assertThat(encryption.encrypt(null)).isNull();

        // decrypt(null) returns DecryptResult with null
        HAEncryption.DecryptResult nullResult = encryption.decrypt(null);
        assertThat(nullResult.plaintext()).isNull();
        assertThat(nullResult.usedSecondaryKey()).isFalse();

        // encrypt("") returns "$ENCRYPTED$..."
        String encryptedEmpty = encryption.encrypt("");
        assertThat(encryptedEmpty).startsWith("$ENCRYPTED$");
        assertThat(encryption.decrypt(encryptedEmpty).plaintext()).isEmpty();
    }

    @Test
    void invalidKeyFormat() {
        assertThatThrownBy(() -> new HAEncryption("not-valid-base64!!!", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Base64");
    }

    @Test
    void invalidKeyLength() {
        // 128-bit key (16 bytes) instead of required 256-bit (32 bytes)
        String shortKey = Base64.getEncoder().encodeToString(new byte[16]);
        assertThatThrownBy(() -> new HAEncryption(shortKey, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("256 bits");
    }

    @Test
    void largePlaintextRoundTrip() {
        String key = generateBase64Key();
        HAEncryption encryption = new HAEncryption(key, null);

        // Build large JSON payload
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) sb.append(",");
            sb.append("{\"event_").append(i).append("\": \"value_").append(i).append("\"}");
        }
        sb.append("]");
        String largePlaintext = sb.toString();

        String encrypted = encryption.encrypt(largePlaintext);
        assertThat(encrypted).startsWith("$ENCRYPTED$");

        HAEncryption.DecryptResult result = encryption.decrypt(encrypted);
        assertThat(result.plaintext()).isEqualTo(largePlaintext);
    }

    @Test
    void isEncryptedStaticHelper() {
        assertThat(HAEncryption.isEncrypted(null)).isFalse();
        assertThat(HAEncryption.isEncrypted("")).isFalse();
        assertThat(HAEncryption.isEncrypted("{\"key\": \"value\"}")).isFalse();
        assertThat(HAEncryption.isEncrypted("$ENCRYPTED$abc123")).isTrue();
    }
}
