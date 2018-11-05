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
package org.locationtech.geowave.analytic.spark.sparksql.udf;

import org.apache.spark.sql.api.java.UDF2;
import org.locationtech.geowave.analytic.spark.sparksql.udf.BufferOperation;
import org.locationtech.geowave.analytic.spark.sparksql.util.GeomReader;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings
public abstract class GeomFunction implements
		UDF2<Geometry, Geometry, Boolean>,
		BufferOperation
{
	private GeomReader geomReader = new GeomReader();

	// Base GeomFunction will assume same bucket comparison
	public double getBufferAmount() {
		return 0.0;
	}

	@Override
	public Boolean call(
			Geometry t1,
			Geometry t2 )
			throws Exception {
		return apply(
				t1,
				t2);
	}

	public abstract boolean apply(
			Geometry geom1,
			Geometry geom2 );

	public String getRegisterName() {
		return this.getClass().getSimpleName();
	}
}
