/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.test.services.grpc;

import org.locationtech.geowave.test.ZookeeperTestEnvironment;
import org.locationtech.geowave.test.mapreduce.MapReduceTestEnvironment;

import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

public class GeoWaveGrpcTestUtils
{
	public final static String typeName = "TestGeometry";
	public final static String indexName = "grpc-spatial";
	public final static String storeName = "grpc";
	public final static String outputStoreName = "grpc-output";
	public final static String cqlSpatialQuery = "BBOX(geometry,0.0,0.0, 25.0, 25.0)";
	public static final String wktSpatialQuery = "POLYGON (( " + "0.0 0.0, " + "0.0 25.0, " + "25.0 25.0, "
			+ "25.0 0.0, " + "0.0 0.0" + "))";
	public static byte[] wkbSpatialQuery;
	static {
		try {
			wkbSpatialQuery = new WKBWriter().write(new WKTReader().read(wktSpatialQuery));
		}
		catch (final ParseException e) {
			e.printStackTrace();
		}
	}
	public final static String temporalQueryStartTime = "2016-02-20T01:32Z";
	public final static String temporalQueryEndTime = "2016-02-21T01:32Z";

	// this is purely a convenience method so the gRPC test client does not need
	// any dependency on the test environment directly
	public static MapReduceTestEnvironment getMapReduceTestEnv() {
		return MapReduceTestEnvironment.getInstance();
	}

	public static ZookeeperTestEnvironment getZookeeperTestEnv() {
		return ZookeeperTestEnvironment.getInstance();
	}
}
