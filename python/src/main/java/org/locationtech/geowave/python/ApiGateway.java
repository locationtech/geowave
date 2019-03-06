package org.locationtech.geowave.python;

import org.locationtech.geowave.examples.ingest.SimpleIngest;
import py4j.GatewayServer;

public class ApiGateway {

  /**
   * Declaring public fields which act as "submodules"
   */

  public Debug debug = new Debug();
  public DataStoreInterfacer storeInterfacer = new DataStoreInterfacer();
  // Expose the simpleIngest example code
  public SimpleIngest simpleIngest = new SimpleIngest();

  public static void main(String[] args) {
    GatewayServer server = new GatewayServer(new ApiGateway());
    GatewayServer.turnLoggingOn();
    server.start();
  }

}
