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
