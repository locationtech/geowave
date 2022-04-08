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

import org.apache.commons.codec.binary.Base64;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.internal.Console;

/** Encryption/Decryption implementation based of symmetric cryptography */
public class GeoWaveEncryption extends BaseEncryption {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveEncryption.class);

  /**
   * Base constructor for encryption, allowing a resource location for the cryptography token key to
   * be specified, rather than using the default-generated path
   *
   * @param resourceLocation Path to cryptography token key file
   */
  public GeoWaveEncryption(final String resourceLocation, Console console) {
    super(resourceLocation, console);
  }

  /** Base constructor for encryption */
  public GeoWaveEncryption(Console console) {
    super(console);
  }

  @Override
  public byte[] encryptBytes(final byte[] valueToEncrypt) throws Exception {
    return Base64.encodeBase64(encryptValue(valueToEncrypt));
  }

  @Override
  public byte[] decryptBytes(final byte[] valueToDecrypt) throws Exception {
    return decryptValue(Base64.decodeBase64(valueToDecrypt));
  }

  private PaddedBufferedBlockCipher getCipher(final boolean encrypt) {
    final PaddedBufferedBlockCipher cipher =
        new PaddedBufferedBlockCipher(new CBCBlockCipher(new AESEngine()), new PKCS7Padding());
    final CipherParameters ivAndKey =
        new ParametersWithIV(new KeyParameter(getKey().getEncoded()), salt);
    cipher.init(encrypt, ivAndKey);
    return cipher;
  }

  /**
   * Encrypts a binary value using the given key and returns a base 64 encoded encrypted string.
   *
   * @param valueToEncrypt Binary value to encrypt
   * @return Encrypted binary
   * @throws Exception
   */
  private byte[] encryptValue(final byte[] encodedValue) throws Exception {
    LOGGER.trace("ENTER :: encyrpt");

    final PaddedBufferedBlockCipher cipher = getCipher(true);
    final byte output[] = new byte[cipher.getOutputSize(encodedValue.length)];
    final int length = cipher.processBytes(encodedValue, 0, encodedValue.length, output, 0);
    try {
      cipher.doFinal(output, length);
    } catch (final CryptoException e) {
      LOGGER.error("An error occurred performing encryption: " + e.getLocalizedMessage(), e);
    }
    return output;
  }

  /**
   * Decrypts the base64-decoded value
   *
   * @param decodedValue value to decrypt
   * @return
   * @throws Exception
   */
  private byte[] decryptValue(final byte[] decodedValue) throws Exception {

    final StringBuffer result = new StringBuffer();

    final PaddedBufferedBlockCipher cipher = getCipher(false);
    final byte output[] = new byte[cipher.getOutputSize(decodedValue.length)];
    final int length = cipher.processBytes(decodedValue, 0, decodedValue.length, output, 0);
    cipher.doFinal(output, length);
    if ((output != null) && (output.length != 0)) {
      final String retval = new String(output, "UTF-8");
      for (int i = 0; i < retval.length(); i++) {
        final char c = retval.charAt(i);
        if (c != 0) {
          result.append(c);
        }
      }
    }
    return result.toString().getBytes("UTF-8");
  }
}
