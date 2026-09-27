/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;
import org.carlspring.cloud.storage.s3fs.S3Factory;
import org.junit.After;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.CredentialUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class GeoWaveAmazonS3FactoryTest {
  private final Map<String, String> previousProperties = new HashMap<>();

  @After
  public void restoreSystemProperties() {
    previousProperties.forEach((property, previous) -> {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    });
  }

  @Test
  public void regionComesFromARegionalEndpoint() {
    assertEquals(
        Optional.of("us-west-2"),
        GeoWaveAmazonS3Factory.regionOf("s3.us-west-2.amazonaws.com"));
    assertEquals(
        Optional.of("us-west-2"),
        GeoWaveAmazonS3Factory.regionOf("s3-us-west-2.amazonaws.com"));
    assertEquals(
        Optional.of("eu-central-1"),
        GeoWaveAmazonS3Factory.regionOf("s3.dualstack.eu-central-1.amazonaws.com"));
    assertEquals(
        Optional.of("cn-north-1"),
        GeoWaveAmazonS3Factory.regionOf("s3.cn-north-1.amazonaws.com.cn"));
  }

  @Test
  public void noRegionComesFromOtherEndpoints() {
    assertEquals(Optional.empty(), GeoWaveAmazonS3Factory.regionOf("s3.amazonaws.com"));
    assertEquals(Optional.empty(), GeoWaveAmazonS3Factory.regionOf("s3-external-1.amazonaws.com"));
    assertEquals(Optional.empty(), GeoWaveAmazonS3Factory.regionOf("minio.example.com"));
    assertEquals(Optional.empty(), GeoWaveAmazonS3Factory.regionOf("127.0.0.1"));
    assertEquals(Optional.empty(), GeoWaveAmazonS3Factory.regionOf(null));
  }

  @Test
  public void clientForARegionalEndpointSignsForItsRegion() {
    try (S3Client client =
        new GeoWaveAmazonS3Factory().getS3Client(
            URI.create("s3://s3.eu-west-1.amazonaws.com"),
            new Properties())) {
      assertEquals(Region.EU_WEST_1, client.serviceClientConfiguration().region());
    }
  }

  @Test
  public void configuredRegionWinsOverTheEndpoints() {
    final Properties props = new Properties();
    props.setProperty(S3Factory.REGION, "eu-north-1");
    try (S3Client client =
        new GeoWaveAmazonS3Factory().getS3Client(
            URI.create("s3://s3.eu-west-1.amazonaws.com"),
            props)) {
      assertEquals(Region.EU_NORTH_1, client.serviceClientConfiguration().region());
    }
  }

  @Test
  public void anonymousWhenNoCredentialsAreFound() {
    assumeNoCredentialsInTheEnvironment();
    setSystemProperty("aws.sharedCredentialsFile", "/nonexistent/credentials");
    setSystemProperty("aws.configFile", "/nonexistent/config");
    setSystemProperty("aws.disableEc2Metadata", "true");
    assertTrue(CredentialUtils.isAnonymous(resolveDefaultCredentials()));
  }

  /** SDK v2 reads {@code aws.secretAccessKey}, where v1 read {@code aws.secretKey}. */
  @Test
  public void defaultChainReadsTheV2SystemProperties() {
    setSystemProperty("aws.accessKeyId", "access");
    setSystemProperty("aws.secretAccessKey", "secret");
    final AwsCredentials credentials = resolveDefaultCredentials();
    assertEquals("access", credentials.accessKeyId());
    assertEquals("secret", credentials.secretAccessKey());
  }

  @Test
  public void configuredKeysAreUsedAsGiven() {
    final Properties props = new Properties();
    props.setProperty(S3Factory.ACCESS_KEY, "access");
    props.setProperty(S3Factory.SECRET_KEY, "secret");
    final AwsCredentials credentials =
        new GeoWaveAmazonS3Factory().getCredentialsProvider(props).resolveCredentials();
    assertEquals("access", credentials.accessKeyId());
    assertEquals("secret", credentials.secretAccessKey());
  }

  private static AwsCredentials resolveDefaultCredentials() {
    return new GeoWaveAmazonS3Factory().getCredentialsProvider(
        new Properties()).resolveCredentials();
  }

  private static void assumeNoCredentialsInTheEnvironment() {
    assumeTrue(
        Stream.of(
            "AWS_ACCESS_KEY_ID",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI").allMatch(name -> System.getenv(name) == null));
  }

  private void setSystemProperty(final String property, final String value) {
    previousProperties.putIfAbsent(property, System.getProperty(property));
    System.setProperty(property, value);
  }
}
