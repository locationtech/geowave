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
package mil.nga.giat.geowave.analytic.spark.sparksql.udt;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.sql.types.DataType;

/**
 * Created by jwileczek on 7/20/18.
 */
public class GeometryUDT extends
		AbstractGeometryUDT<Geometry>
{

	@Override
	public boolean acceptsType(
			DataType dataType ) {
		return super.acceptsType(dataType) || dataType.getClass() == GeometryUDT.class
				|| dataType.getClass() == PointUDT.class || dataType.getClass() == LineStringUDT.class
				|| dataType.getClass() == PolygonUDT.class || dataType.getClass() == MultiLineStringUDT.class
				|| dataType.getClass() == MultiPointUDT.class || dataType.getClass() == MultiPolygonUDT.class;
	}

	@Override
	public Class<Geometry> userClass() {
		return Geometry.class;
	}
}
