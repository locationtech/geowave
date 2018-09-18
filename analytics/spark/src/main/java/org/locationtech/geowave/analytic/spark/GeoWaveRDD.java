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
package org.locationtech.geowave.analytic.spark;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.opengis.feature.simple.SimpleFeature;

public class GeoWaveRDD implements
		Serializable
{
	private JavaPairRDD<GeoWaveInputKey, SimpleFeature> rawRDD = null;

	public GeoWaveRDD() {}

	public GeoWaveRDD(
			final JavaPairRDD<GeoWaveInputKey, SimpleFeature> rawRDD ) {
		this.rawRDD = rawRDD;
	}

	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> getRawRDD() {
		return rawRDD;
	}

	public void setRawRDD(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rawRDD ) {
		this.rawRDD = rawRDD;
	}

	public boolean isLoaded() {
		return (getRawRDD() != null);
	}

}
