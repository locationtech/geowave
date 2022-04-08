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
import java.net.URL;
import org.apache.commons.lang.StringUtils;

public class S3ParamsExtractor {

  protected static S3Params extract(final URL url) throws IOException, IllegalArgumentException {

    if (!"s3".equals(url.getProtocol())) {
      throw new IllegalArgumentException("Unsupported protocol '" + url.getProtocol() + "'");
    }

    // bucket
    final int index = StringUtils.ordinalIndexOf(url.getPath(), "/", 2);
    final String bucket = url.getPath().substring(1, index);

    // key
    final String key = url.getPath().substring(index + 1);

    return new S3Params(bucket, key);
  }
}
