/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.ingest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

/**
 * This SPI interface is used to circumvent the need of core store to require HDFS or S3 libraries.
 * However, if libraries are on the classpath, it will handle URLs from hdfs and S3 appropriately.
 *
 *
 */
public interface IngestUrlHandlerSpi {
  public Path handlePath(String path, Properties configProperties) throws IOException;
}
