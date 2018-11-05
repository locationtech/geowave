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

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by jwileczek on 8/16/18.
 */
public class GeomFromWKT implements
		UDF1<String, Geometry>
{

	@Override
	public Geometry call(
			String o )
			throws Exception {
		return new WKTReader().read(o);
	}

}
