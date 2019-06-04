package org.locationtech.geowave.python;

import org.apache.spark.sql.sources.In;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.opengis.feature.simple.SimpleFeatureType;
import org.locationtech.jts.geom.Coordinate;
import py4j.GatewayServer;

public class ApiGateway {

  /**
   * Declaring public fields which act as "submodules"
   */

  private Debug debug = new Debug();
  // Expose the simpleIngest example code
  private SimpleIngest simpleIngest = new SimpleIngest();

  public Debug getDebug() {
    return debug;
  }

  public SimpleIngest getSimpleIngest() {
    return simpleIngest;
  }

  public static void main(String[] args) {
    GatewayServer server = new GatewayServer(new ApiGateway());
    GatewayServer.turnLoggingOn();

    server.start();
  }

}
