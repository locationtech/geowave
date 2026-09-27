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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;

/**
 * Reads an {@code s3://<endpoint>/<bucket>/<key>} URL, the form an S3 path's {@code toUri()} takes,
 * through the same filesystem and client that listed it, so the endpoint and every {@code s3fs.*}
 * setting apply to reading too.
 */
public class S3URLConnection extends URLConnection {

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
    final URI uri;
    try {
      uri = url.toURI();
    } catch (final URISyntaxException e) {
      throw new IOException("Invalid S3 URL " + url, e);
    }
    return Files.newInputStream(GeoWaveAmazonS3Factory.getFileSystem(uri).getPath(uri.getPath()));
  }

  @Override
  public void connect() throws IOException {
    // do nothing
  }
}
