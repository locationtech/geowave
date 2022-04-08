/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/** */
package org.locationtech.geowave.core.cli.operations.config.security.crypto;

import java.io.File;
import java.security.Key;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import org.locationtech.geowave.core.cli.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.internal.Console;

/**
 * Abstract base encryption class for setting up and defining common encryption/decryption methods
 */
public abstract class BaseEncryption {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseEncryption.class);

  public static String resourceName = "geowave_crypto_key.dat";
  private String resourceLocation;
  private Key key = null;

  /*
   * PROTECT this value. The salt value is the second-half of the protection mechanism for key used
   * when encrypting or decrypting the content.<br/> We cannot generate a new random cryptography
   * key each time, as that would mean two different keys.<br/> At the same time, encrypted values
   * would be very vulnerable to unintentional exposure if (a wrong) someone got access to the token
   * key file, so this salt allows us to protect the encryption with "2 locks" - both are needed to
   * decrypt a value that was encrypted with the SAME two values (salt - below - and token file -
   * specified at resourceLocation)
   */
  protected byte[] salt = null;
  protected File tokenFile = null;

  private static final String PREFIX = "ENC{";
  private static final String SUFFIX = "}";
  public static final String WRAPPER = PREFIX + SUFFIX;
  private static final Pattern ENCCodePattern =
      Pattern.compile(PREFIX.replace("{", "\\{") + "([^}]+)" + SUFFIX.replace("{", "\\{"));

  private final String KEY_ENCRYPTION_ALGORITHM = "AES";

  /**
   * Base constructor for encryption, allowing a resource location for the cryptography token key to
   * be specified, rather than using the default-generated path
   *
   * @param resourceLocation Path to cryptography token key file
   */
  public BaseEncryption(final String resourceLocation, Console console) {
    try {
      setResourceLocation(resourceLocation);
      init(console);
    } catch (final Throwable t) {
      LOGGER.error(t.getLocalizedMessage(), t);
    }
  }

  /** Base constructor for encryption */
  public BaseEncryption(Console console) {
    init(console);
  }

  /**
   * Method to initialize all required fields, check for the existence of the cryptography token
   * key, and generate the key for encryption/decryption
   */
  private void init(Console console) {
    try {
      checkForToken(console);
      setResourceLocation(tokenFile.getCanonicalPath());

      salt = "Ge0W@v3-Ro0t-K3y".getBytes("UTF-8");

      generateRootKeyFromToken();
    } catch (final Throwable t) {
      LOGGER.error(t.getLocalizedMessage(), t);
    }
  }

  /** Check if encryption token exists. If not, create one initially */
  private void checkForToken(Console console) throws Throwable {
    if (getResourceLocation() != null) {
      // this is simply caching the location, ideally under all
      // circumstances resource location exists
      tokenFile = new File(getResourceLocation());
    } else {
      // and this is initializing it for the first time, this just assumes
      // the default config file path
      // because of that assumption this can cause inconsistency
      // under all circumstances this seems like it should never happen
      tokenFile =
          SecurityUtils.getFormattedTokenKeyFileForConfig(
              ConfigOptions.getDefaultPropertyFile(console));
    }
    if (!tokenFile.exists()) {
      generateNewEncryptionToken(tokenFile);
    }
  }

  /**
   * Generates a token file resource name that includes the current version
   *
   * @return formatted token key file name
   */
  public static String getFormattedTokenFileName(final String configFilename) {
    return String.format("%s.key", configFilename);
  }

  /**
   * Generate a new token value in a specified file
   *
   * @param tokenFile
   * @return {@code true} if the token was successfully generated
   */
  public static boolean generateNewEncryptionToken(final File tokenFile) throws Exception {
    boolean success = false;
    try {
      LOGGER.info("Writing new encryption token to file at path {}", tokenFile.getCanonicalPath());
      org.apache.commons.io.FileUtils.writeStringToFile(tokenFile, generateRandomSecretKey());
      LOGGER.info("Completed writing new encryption token to file");
      success = true;
    } catch (final Exception ex) {
      LOGGER.error(
          "An error occurred writing new encryption token to file: " + ex.getLocalizedMessage(),
          ex);
      throw ex;
    }
    return success;
  }

  /*
   * INTERNAL METHODS
   */
  /**
   * Returns the path on the file system to the resource for the token
   *
   * @return Path to resource to get the token
   */
  public String getResourceLocation() {
    return resourceLocation;
  }

  /**
   * Sets the path to the resource for the token
   *
   * @param resourceLoc Path to resource to get the token
   */
  public void setResourceLocation(final String resourceLoc) throws Throwable {
    resourceLocation = resourceLoc;
  }

  /**
   * Checks to see if the data is properly wrapped with ENC{}
   *
   * @param data
   * @return boolean - true if properly wrapped, false otherwise
   */
  public static boolean isProperlyWrapped(final String data) {
    return ENCCodePattern.matcher(data).matches();
  }

  /**
   * Converts a binary value to a encoded string
   *
   * @param data Binary value to encode as an encoded string
   * @return Encoded string from the binary value specified
   */
  private String toString(final byte[] data) {
    return Hex.encodeHexString(data);
  }

  /**
   * Converts a string value to a decoded binary
   *
   * @param data String value to convert to decoded hex
   * @return Decoded binary from the string value specified
   */
  private byte[] fromString(final String data) {
    try {
      return Hex.decodeHex(data.toCharArray());
    } catch (final DecoderException e) {
      LOGGER.error(e.getLocalizedMessage(), e);
      return null;
    }
  }

  /** Method to generate a new secret key from the specified token key file */
  private void generateRootKeyFromToken() throws Throwable {

    if (!tokenFile.exists()) {
      throw new Throwable("Token file not found at specified path [" + getResourceLocation() + "]");
    }
    try {
      final String strPassword = FileUtils.readFileContent(tokenFile);
      final char[] password = strPassword != null ? strPassword.trim().toCharArray() : null;
      final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
      final SecretKey tmp = factory.generateSecret(new PBEKeySpec(password, salt, 65536, 256));
      setKey(new SecretKeySpec(tmp.getEncoded(), KEY_ENCRYPTION_ALGORITHM));
    } catch (final Exception ex) {
      LOGGER.error(
          "An error occurred generating the root key from the specified token: "
              + ex.getLocalizedMessage(),
          ex);
    }
  }

  /**
   * Method to generate a new random token key value
   *
   * @return
   * @throws Exception
   */
  private static String generateRandomSecretKey() throws Exception {
    final KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256);
    final SecretKey secretKey = keyGenerator.generateKey();
    final byte[] encoded = secretKey.getEncoded();
    return Base64.encodeBase64String(encoded);
  }

  /**
   * Set the key to use
   *
   * @param key
   */
  protected void setKey(final Key key) {
    this.key = key;
  }

  /**
   * @return the key to use
   */
  protected Key getKey() {
    return key;
  }

  /*
   * ENCRYPTION METHODS
   */
  /**
   * Method to encrypt and hex-encode a string value using the specified token resource
   *
   * @param data String to encrypt
   * @return Encrypted and Hex-encoded string value using the specified token resource
   * @throws Exception
   */
  public String encryptAndHexEncode(final String data) throws Exception {
    if (data == null) {
      return null;
    }
    final byte[] encryptedBytes = encryptBytes(data.getBytes("UTF-8"));
    return PREFIX + toString(encryptedBytes) + SUFFIX;
  }

  /*
   * DECRYPTION METHODS
   */
  /**
   * Returns a decrypted value from the encrypted hex-encoded value specified
   *
   * @param data Hex-Encoded string value to decrypt
   * @return Decrypted value from the encrypted hex-encoded value specified
   * @throws Exception
   */
  public String decryptHexEncoded(final String data) throws Exception {
    if (data == null) {
      return null;
    }
    final Matcher matcher = ENCCodePattern.matcher(data);
    if (matcher.matches()) {
      final String codedString = matcher.group(1);
      return new String(decryptBytes(fromString(codedString)), "UTF-8");
    } else {
      return data;
    }
  }

  /*
   * ABSTRACT METHODS
   */
  /**
   * Encrypt the data as a byte array
   *
   * @param valueToEncrypt value to encrypt
   */
  public abstract byte[] encryptBytes(byte[] valueToEncrypt) throws Exception;

  /**
   * Decrypt the encrypted data
   *
   * @param valueToDecrypt value to encrypt
   */
  public abstract byte[] decryptBytes(byte[] valueToDecrypt) throws Exception;
}
