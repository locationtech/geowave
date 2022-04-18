/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/** */
package org.locationtech.geowave.cli.geoserver;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Properties;
import org.locationtech.geowave.core.cli.converters.OptionalPasswordConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;

/** */
public abstract class StoreSSLConfigurationOptions {
  private static final Logger LOGGER = LoggerFactory.getLogger(StoreSSLConfigurationOptions.class);

  private final String configPrefix;

  public StoreSSLConfigurationOptions(final String configPrefix) {
    this.configPrefix = configPrefix;
  }

  @SSLOptionAnnotation(propertyBaseName = "ssl.security.protocol")
  @Parameter(
      names = "--sslSecurityProtocol",
      description = "Specify the Transport Layer Security (TLS) protocol to use when connecting to the server. By default, the system will use TLS.")
  protected String sslSecurityProtocol;

  @SSLOptionAnnotation(propertyBaseName = "ssl.trustStore")
  @Parameter(
      names = "--sslTrustStorePath",
      description = "Specify the absolute path to where truststore file is located on system. The truststore file is used to validate client certificates.")
  protected String sslTrustStorePath;

  @SSLOptionAnnotation(propertyBaseName = "ssl.trustStorePassword")
  @Parameter(
      names = "--sslTrustStorePassword",
      description = "Specify the password to use to access the truststore file. - "
          + OptionalPasswordConverter.DEFAULT_PASSWORD_DESCRIPTION,
      converter = OptionalPasswordConverter.class)
  protected String sslTrustStorePassword;

  @SSLOptionAnnotation(propertyBaseName = "ssl.trustStoreType")
  @Parameter(
      names = "--sslTrustStoreType",
      description = "Specify the type of key store used for the truststore, i.e. JKS (Java KeyStore).")
  protected String sslTrustStoreType;

  @SSLOptionAnnotation(propertyBaseName = "ssl.trustStoreProvider")
  @Parameter(
      names = "--sslTrustStoreProvider",
      description = "Specify the name of the truststore provider to be used for the server certificate.")
  protected String sslTrustStoreProvider;

  @SSLOptionAnnotation(propertyBaseName = "ssl.trustStoreMgrFactoryAlgorithm")
  @Parameter(
      names = "--sslTrustManagerAlgorithm",
      description = "Specify the algorithm to use for the truststore.")
  protected String sslTrustManagerAlgorithm;

  @SSLOptionAnnotation(propertyBaseName = "ssl.trustStoreMgrFactoryProvider")
  @Parameter(
      names = "--sslTrustManagerProvider",
      description = "Specify the trust manager factory provider.")
  protected String sslTrustManagerProvider;

  @SSLOptionAnnotation(propertyBaseName = "ssl.keyStore")
  @Parameter(
      names = "--sslKeyStorePath",
      description = "Specify the absolute path to where the keystore file is located on system. The keystore contains the server certificate to be loaded.")
  protected String sslKeyStorePath;

  @SSLOptionAnnotation(propertyBaseName = "ssl.keyStorePassword")
  @Parameter(
      names = "--sslKeyStorePassword",
      description = "Specify the password to use to access the keystore file. - "
          + OptionalPasswordConverter.DEFAULT_PASSWORD_DESCRIPTION,
      converter = OptionalPasswordConverter.class)
  protected String sslKeyStorePassword;

  @SSLOptionAnnotation(propertyBaseName = "ssl.keyStoreProvider")
  @Parameter(
      names = "--sslKeyStoreProvider",
      description = "Specify the name of the keystore provider to be used for the server certificate.")
  protected String sslKeyStoreProvider;

  @SSLOptionAnnotation(propertyBaseName = "ssl.keyPassword")
  @Parameter(
      names = "--sslKeyPassword",
      description = "Specify the password to be used to access the server certificate from the specified keystore file. - "
          + OptionalPasswordConverter.DEFAULT_PASSWORD_DESCRIPTION,
      converter = OptionalPasswordConverter.class)
  protected String sslKeyPassword;

  @SSLOptionAnnotation(propertyBaseName = "ssl.keyStoreType")
  @Parameter(
      names = "--sslKeyStoreType",
      description = "The type of keystore file to be used for the server certificate.")
  protected String sslKeyStoreType;

  @SSLOptionAnnotation(propertyBaseName = "ssl.keyMgrFactoryAlgorithm")
  @Parameter(
      names = "--sslKeyManagerAlgorithm",
      description = "Specify the algorithm to use for the keystore.")
  protected String sslKeyManagerAlgorithm;

  @SSLOptionAnnotation(propertyBaseName = "ssl.keyMgrFactoryProvider")
  @Parameter(
      names = "--sslKeyManagerProvider",
      description = "Specify the key manager factory provider.")
  protected String sslKeyManagerProvider;

  /** @return the sslSecurityProtocol */
  public String getSslSecurityProtocol() {
    return sslSecurityProtocol;
  }

  /** @param sslSecurityProtocol the sslSecurityProtocol to set */
  public void setSslSecurityProtocol(final String sslSecurityProtocol) {
    this.sslSecurityProtocol = sslSecurityProtocol;
  }

  /** @return the sslTrustStorePath */
  public String getSslTrustStorePath() {
    return sslTrustStorePath;
  }

  /** @param sslTrustStorePath the sslTrustStorePath to set */
  public void setSslTrustStorePath(final String sslTrustStorePath) {
    this.sslTrustStorePath = sslTrustStorePath;
  }

  /** @return the sslTrustStorePassword */
  public String getSslTrustStorePassword() {
    return sslTrustStorePassword;
  }

  /** @param sslTrustStorePassword the sslTrustStorePassword to set */
  public void setSslTrustStorePassword(final String sslTrustStorePassword) {
    this.sslTrustStorePassword = sslTrustStorePassword;
  }

  /** @return the sslTrustStoreType */
  public String getSslTrustStoreType() {
    return sslTrustStoreType;
  }

  /** @param sslTrustStoreType the sslTrustStoreType to set */
  public void setSslTrustStoreType(final String sslTrustStoreType) {
    this.sslTrustStoreType = sslTrustStoreType;
  }

  /** @return the sslTrustStoreProvider */
  public String getSslTrustStoreProvider() {
    return sslTrustStoreProvider;
  }

  /** @param sslTrustStoreProvider the sslTrustStoreProvider to set */
  public void setSslTrustStoreProvider(final String sslTrustStoreProvider) {
    this.sslTrustStoreProvider = sslTrustStoreProvider;
  }

  /** @return the sslTrustManagerAlgorithm */
  public String getSslTrustManagerAlgorithm() {
    return sslTrustManagerAlgorithm;
  }

  /** @param sslTrustManagerAlgorithm the sslTrustManagerAlgorithm to set */
  public void setSslTrustManagerAlgorithm(final String sslTrustManagerAlgorithm) {
    this.sslTrustManagerAlgorithm = sslTrustManagerAlgorithm;
  }

  /** @return the sslTrustManagerProvider */
  public String getSslTrustManagerProvider() {
    return sslTrustManagerProvider;
  }

  /** @param sslTrustManagerProvider the sslTrustManagerProvider to set */
  public void setSslTrustManagerProvider(final String sslTrustManagerProvider) {
    this.sslTrustManagerProvider = sslTrustManagerProvider;
  }

  /** @return the sslKeyStorePath */
  public String getSslKeyStorePath() {
    return sslKeyStorePath;
  }

  /** @param sslKeyStorePath the sslKeyStorePath to set */
  public void setSslKeyStorePath(final String sslKeyStorePath) {
    this.sslKeyStorePath = sslKeyStorePath;
  }

  /** @return the sslKeyStorePassword */
  public String getSslKeyStorePassword() {
    return sslKeyStorePassword;
  }

  /** @param sslKeyStorePassword the sslKeyStorePassword to set */
  public void setSslKeyStorePassword(final String sslKeyStorePassword) {
    this.sslKeyStorePassword = sslKeyStorePassword;
  }

  /** @return the sslKeyStoreProvider */
  public String getSslKeyStoreProvider() {
    return sslKeyStoreProvider;
  }

  /** @param sslKeyStoreProvider the sslKeyStoreProvider to set */
  public void setSslKeyStoreProvider(final String sslKeyStoreProvider) {
    this.sslKeyStoreProvider = sslKeyStoreProvider;
  }

  /** @return the sslKeyPassword */
  public String getSslKeyPassword() {
    return sslKeyPassword;
  }

  /** @param sslKeyPassword the sslKeyPassword to set */
  public void setSslKeyPassword(final String sslKeyPassword) {
    this.sslKeyPassword = sslKeyPassword;
  }

  /** @return the sslKeyStoreType */
  public String getSslKeyStoreType() {
    return sslKeyStoreType;
  }

  /** @param sslKeyStoreType the sslKeyStoreType to set */
  public void setSslKeyStoreType(final String sslKeyStoreType) {
    this.sslKeyStoreType = sslKeyStoreType;
  }

  /** @return the sslKeyManagerAlgorithm */
  public String getSslKeyManagerAlgorithm() {
    return sslKeyManagerAlgorithm;
  }

  /** @param sslKeyManagerAlgorithm the sslKeyManagerAlgorithm to set */
  public void setSslKeyManagerAlgorithm(final String sslKeyManagerAlgorithm) {
    this.sslKeyManagerAlgorithm = sslKeyManagerAlgorithm;
  }

  /** @return the sslKeyManagerProvider */
  public String getSslKeyManagerProvider() {
    return sslKeyManagerProvider;
  }

  /** @param sslKeyManagerProvider the sslKeyManagerProvider to set */
  public void setSslKeyManagerProvider(final String sslKeyManagerProvider) {
    this.sslKeyManagerProvider = sslKeyManagerProvider;
  }

  public boolean saveProperties(final Properties existingProps) {
    boolean updated = false;
    final Field[] fields = StoreSSLConfigurationOptions.class.getDeclaredFields();
    if ((fields != null) && (fields.length != 0)) {
      for (final Field field : fields) {
        field.setAccessible(true); // HPFortify
        // "Access Specifier Manipulation"
        // False Positive: These fields are being modified by trusted
        // code,
        // in a way that is not influenced by user input
        final Annotation[] annotations = field.getAnnotations();
        for (final Annotation annotation : annotations) {
          if (annotation instanceof SSLOptionAnnotation) {
            final SSLOptionAnnotation sslOptionAnnotation = (SSLOptionAnnotation) annotation;
            Object value = null;
            try {
              value = field.get(this);
            } catch (IllegalArgumentException | IllegalAccessException e) {
              LOGGER.error(e.getLocalizedMessage(), e);
            }
            // only write to properties the values which have been
            // specified
            if ((value != null) && (sslOptionAnnotation.propertyBaseName() != null)) {
              final String propertyKey =
                  String.format("%s.%s", configPrefix, sslOptionAnnotation.propertyBaseName());
              existingProps.put(propertyKey, value);
              updated = true;
            }
          }
        }
      }
    }
    return updated;
  }
}
