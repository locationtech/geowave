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
package org.locationtech.geowave.core.geotime.store.dimension;

import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.NumericData;

/**
 * This field can be used as a EPSG:4326 latitude dimension within GeoWave. It
 * can utilize JTS geometry as the underlying spatial object for this dimension.
 *
 */
public class LatitudeField extends
		SpatialField
{
	public LatitudeField() {
		this(
				GeometryWrapper.DEFAULT_GEOMETRY_FIELD_NAME);
	}

	public LatitudeField(
			final boolean useHalfRange ) {
		this(
				useHalfRange,
				GeometryWrapper.DEFAULT_GEOMETRY_FIELD_NAME);
	}

	public LatitudeField(
			final String fieldName ) {
		this(
				false,
				fieldName);
	}

	public LatitudeField(
			final boolean useHalfRange,
			final String fieldName ) {
		this(
				new LatitudeDefinition(
						useHalfRange),
				fieldName);
	}

	public LatitudeField(
			final NumericDimensionDefinition baseDefinition,
			final String fieldName ) {
		super(
				baseDefinition,
				fieldName);
	}

	@Override
	public NumericData getNumericData(
			final GeometryWrapper geometry ) {
		return GeometryUtils.yRangeFromGeometry(geometry.getGeometry());
	}
}
