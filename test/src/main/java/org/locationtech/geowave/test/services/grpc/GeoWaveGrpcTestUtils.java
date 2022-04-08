/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services.grpc;

import org.locationtech.geowave.test.ZookeeperTestEnvironment;
import org.locationtech.geowave.test.mapreduce.MapReduceTestEnvironment;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

public class GeoWaveGrpcTestUtils {
  public static final String typeName = "TestGeometry";
  public static final String indexName = "grpcspatial";
  public static final String storeName = "grpc";
  public static final String outputStoreName = "grpc-output";
  public static final String cqlSpatialQuery = "BBOX(geometry,0.0,0.0, 25.0, 25.0)";
  public static final String wktSpatialQuery =
      "POLYGON (( " + "0.0 0.0, " + "0.0 25.0, " + "25.0 25.0, " + "25.0 0.0, " + "0.0 0.0" + "))";
  public static byte[] wkbSpatialQuery;

  static {
    try {
      wkbSpatialQuery = new WKBWriter().write(new WKTReader().read(wktSpatialQuery));
    } catch (final ParseException e) {
      e.printStackTrace();
    }
  }

  public static final String temporalQueryStartTime = "2016-02-20T01:32Z";
  public static final String temporalQueryEndTime = "2016-02-21T01:32Z";

  // this is purely a convenience method so the gRPC test client does not need
  // any dependency on the test environment directly
  public static MapReduceTestEnvironment getMapReduceTestEnv() {
    return MapReduceTestEnvironment.getInstance();
  }

  public static ZookeeperTestEnvironment getZookeeperTestEnv() {
    return ZookeeperTestEnvironment.getInstance();
  }
}
