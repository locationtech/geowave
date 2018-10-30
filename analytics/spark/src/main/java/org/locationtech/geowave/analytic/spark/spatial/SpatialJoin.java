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
package org.locationtech.geowave.analytic.spark.spatial;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import org.apache.spark.sql.SparkSession;
import org.locationtech.geowave.analytic.spark.GeoWaveIndexedRDD;
import org.locationtech.geowave.analytic.spark.sparksql.udf.GeomFunction;
import org.locationtech.geowave.core.index.NumericIndexStrategy;

public interface SpatialJoin extends
		Serializable
{
	void join(
			SparkSession spark,
			GeoWaveIndexedRDD leftRDD,
			GeoWaveIndexedRDD rightRDD,
			GeomFunction predicate )
			throws InterruptedException,
			ExecutionException;

	boolean supportsJoin(
			NumericIndexStrategy indexStrategy );

	NumericIndexStrategy createDefaultStrategy(
			NumericIndexStrategy indexStrategy );
}
