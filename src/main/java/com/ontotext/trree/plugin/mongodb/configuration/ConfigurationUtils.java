package com.ontotext.trree.plugin.mongodb.configuration;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConfigurationUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationUtils.class);

  private static final String ENC_ALG = "AES";
  private static final String KEY_HASH_ALG = "SHA-1";
  private static final int KEY_LENGTH = 16;

  private ConfigurationUtils() {
    throw new IllegalStateException("Don't do that!");
  }

  static String encrypt(String value, String key) {
    validateInput(value, key);

    try {
      Cipher cipher = getCipher(key, Cipher.ENCRYPT_MODE);
      return new String(Base64.getMimeEncoder().encode(cipher.doFinal(value.getBytes())));
    } catch (Exception exc) {
      LOGGER.error("Could not encrypt mongo value", exc);
      throw new RuntimeException("Could not encrypt value", exc);
    }
  }

  static String decrypt(String value, String key) {
    validateInput(value, key);

    try {
      Cipher cipher = getCipher(key, Cipher.DECRYPT_MODE);
      return new String(cipher.doFinal(Base64.getMimeDecoder().decode(value)));
    } catch (Exception exc) {
      LOGGER.error("Could not decrypt mongo value", exc);
      throw new RuntimeException("Could not decrypt value", exc);
    }
  }

  private static void validateInput(String text, String key) {
    if (text == null || key == null) {
      throw new RuntimeException("Text or key is null!");
    }
  }

  private static Cipher getCipher(String key, int cipherMode)
      throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
    byte[] hash = Arrays.copyOf(MessageDigest.getInstance(KEY_HASH_ALG).digest(key.getBytes()), KEY_LENGTH);
    Key aesKey = new SecretKeySpec(hash, ENC_ALG);
    Cipher cipher = Cipher.getInstance(ENC_ALG);
    cipher.init(cipherMode, aesKey);
    return cipher;
  }
}
