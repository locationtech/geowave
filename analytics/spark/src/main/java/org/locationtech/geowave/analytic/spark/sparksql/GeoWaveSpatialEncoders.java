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
package org.locationtech.geowave.analytic.spark.sparksql;

import org.locationtech.jts.geom.*;

import org.apache.spark.sql.types.UDTRegistration;
import org.locationtech.geowave.analytic.spark.sparksql.udt.*;

/**
 * Created by jwileczek on 7/24/18.
 */
public class GeoWaveSpatialEncoders
{

	public static GeometryUDT geometryUDT = new GeometryUDT();
	public static PointUDT pointUDT = new PointUDT();
	public static LineStringUDT lineStringUDT = new LineStringUDT();
	public static PolygonUDT polygonUDT = new PolygonUDT();
	public static MultiPointUDT multiPointUDT = new MultiPointUDT();
	public static MultiPolygonUDT multiPolygonUDT = new MultiPolygonUDT();

	public static void registerUDTs() {
		UDTRegistration.register(
				Geometry.class.getCanonicalName(),
				GeometryUDT.class.getCanonicalName());
		UDTRegistration.register(
				Point.class.getCanonicalName(),
				PointUDT.class.getCanonicalName());
		UDTRegistration.register(
				LineString.class.getCanonicalName(),
				LineStringUDT.class.getCanonicalName());
		UDTRegistration.register(
				Polygon.class.getCanonicalName(),
				PolygonUDT.class.getCanonicalName());

		UDTRegistration.register(
				MultiLineString.class.getCanonicalName(),
				MultiLineStringUDT.class.getCanonicalName());
		UDTRegistration.register(
				MultiPoint.class.getCanonicalName(),
				MultiPointUDT.class.getCanonicalName());
		UDTRegistration.register(
				MultiPolygon.class.getCanonicalName(),
				MultiPolygonUDT.class.getCanonicalName());

	}
}
