/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.s3;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;

public class S3URLConnection extends URLConnection {

  public static final String PROP_S3_HANDLER_USER_AGENT = "s3.handler.userAgent";
  public static final String PROP_S3_HANDLER_PROTOCOL = "s3.handler.protocol";
  public static final String PROP_S3_HANDLER_SIGNER_OVERRIDE = "s3.handler.signerOverride";

  /**
   * Constructs a URL connection to the specified URL. A connection to the object referenced by the
   * URL is not created.
   *
   * @param url the specified URL.
   */
  public S3URLConnection(final URL url) {
    super(url);
  }

  @Override
  public InputStream getInputStream() throws IOException {
    final S3Params s3Params = S3ParamsExtractor.extract(url);

    final ClientConfiguration clientConfig = buildClientConfig();

    final AmazonS3 s3Client =
        new AmazonS3Client(new DefaultGeoWaveAWSCredentialsProvider(), clientConfig);

    final S3Object object = s3Client.getObject(s3Params.getBucket(), s3Params.getKey());
    return object.getObjectContent();
  }

  @Override
  public void connect() throws IOException {
    // do nothing
  }

  // -----------------------------------------------------------------------------------------------------------------

  private ClientConfiguration buildClientConfig() {
    final String userAgent = System.getProperty(PROP_S3_HANDLER_USER_AGENT, null);
    final String protocol = System.getProperty(PROP_S3_HANDLER_PROTOCOL, "https");
    final String signerOverride = System.getProperty(PROP_S3_HANDLER_SIGNER_OVERRIDE, null);

    final ClientConfiguration clientConfig =
        new ClientConfiguration().withProtocol(
            "https".equalsIgnoreCase(protocol) ? Protocol.HTTPS : Protocol.HTTP);

    if (userAgent != null) {
      clientConfig.setUserAgent(userAgent);
    }
    if (signerOverride != null) {
      clientConfig.setSignerOverride(signerOverride);
    }

    return clientConfig;
  }
}
