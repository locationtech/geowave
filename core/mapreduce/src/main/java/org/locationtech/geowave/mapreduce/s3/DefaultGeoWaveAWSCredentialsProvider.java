/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

class DefaultGeoWaveAWSCredentialsProvider extends DefaultAWSCredentialsProviderChain {

  @Override
  public AWSCredentials getCredentials() {
    try {
      return super.getCredentials();
    } catch (final SdkClientException exception) {

    }
    // fall back to anonymous credentials
    return new AnonymousAWSCredentials();
  }
}
