/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.s3;

import java.util.Properties;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.upplication.s3fs.AmazonS3ClientFactory;

public class GeoWaveAmazonS3Factory extends AmazonS3ClientFactory {

  @Override
  protected AWSCredentialsProvider getCredentialsProvider(final Properties props) {
    final AWSCredentialsProvider credentialsProvider = super.getCredentialsProvider(props);
    if (credentialsProvider instanceof DefaultAWSCredentialsProviderChain) {
      return new DefaultGeoWaveAWSCredentialsProvider();
    }
    return credentialsProvider;
  }
}
