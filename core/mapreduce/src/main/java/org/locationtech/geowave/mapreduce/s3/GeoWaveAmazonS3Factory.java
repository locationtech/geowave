/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.s3;

import java.net.URI;
import java.nio.file.FileSystemAlreadyExistsException;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.carlspring.cloud.storage.s3fs.S3ClientFactory;
import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class GeoWaveAmazonS3Factory extends S3ClientFactory {
  private static final Pattern REGIONAL_ENDPOINT =
      Pattern.compile("s3[.-](?:dualstack\\.)?([a-z0-9-]+)\\.amazonaws\\.com(?:\\.cn)?");

  /**
   * The filesystem for the S3 endpoint named by an {@code s3://<endpoint>/...} URI, created on
   * first use and shared after that.
   */
  public static S3FileSystem getFileSystem(final URI uri) {
    final URI endpoint = URI.create("s3://" + uri.getRawAuthority());
    final S3FileSystemProvider provider = new S3FileSystemProvider();
    try {
      return (S3FileSystem) provider.getFileSystem(
          endpoint,
          Collections.singletonMap(
              S3FileSystemProvider.S3_FACTORY_CLASS,
              GeoWaveAmazonS3Factory.class.getName()));
    } catch (final FileSystemAlreadyExistsException e) {
      // another thread created it between the lookup and the creation
      return provider.getFileSystem(endpoint);
    }
  }

  @Override
  public S3Client getS3Client(final URI uri, final Properties props) {
    final Optional<String> endpointRegion = regionOf(uri.getHost());
    if (!endpointRegion.isPresent() || props.containsKey(REGION)) {
      return super.getS3Client(uri, props);
    }
    // SDK v1 took the signing region from a regional endpoint's host name; v2 has to be told.
    final Properties withRegion = new Properties();
    withRegion.putAll(props);
    withRegion.setProperty(REGION, endpointRegion.get());
    return super.getS3Client(uri, withRegion);
  }

  @Override
  protected S3Client createS3Client(final S3ClientBuilder builder) {
    // v1 found a bucket's region by itself when it lived outside the endpoint's region
    return builder.crossRegionAccessEnabled(true).build();
  }

  @Override
  protected AwsCredentialsProvider getCredentialsProvider(final Properties props) {
    final AwsCredentialsProvider credentialsProvider = super.getCredentialsProvider(props);
    if (credentialsProvider instanceof DefaultCredentialsProvider) {
      return new DefaultGeoWaveAWSCredentialsProvider();
    }
    return credentialsProvider;
  }

  static Optional<String> regionOf(final String host) {
    if (host == null) {
      return Optional.empty();
    }
    final Matcher matcher = REGIONAL_ENDPOINT.matcher(host);
    if (!matcher.matches()) {
      return Optional.empty();
    }
    final Region region = Region.of(matcher.group(1));
    return Region.regions().contains(region) ? Optional.of(region.id()) : Optional.empty();
  }
}
