/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.s3;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;

/** The SDK's default credentials chain, falling back to anonymous access for public buckets. */
class DefaultGeoWaveAWSCredentialsProvider implements AwsCredentialsProvider {
  private final AwsCredentialsProvider defaultChain = DefaultCredentialsProvider.builder().build();

  @Override
  public AwsCredentials resolveCredentials() {
    try {
      return defaultChain.resolveCredentials();
    } catch (final SdkClientException e) {
      return AnonymousCredentialsProvider.create().resolveCredentials();
    }
  }
}
