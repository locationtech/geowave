/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;

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
				GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID);
	}

	public LatitudeField(
			final boolean useHalfRange ) {
		this(
				useHalfRange,
				GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID);
	}

	public LatitudeField(
			final ByteArrayId fieldId ) {
		this(
				false,
				fieldId);
	}

	public LatitudeField(
			final boolean useHalfRange,
			final ByteArrayId fieldId ) {
		this(
				new LatitudeDefinition(
						useHalfRange),
				fieldId);
	}

	public LatitudeField(
			final NumericDimensionDefinition baseDefinition,
			final ByteArrayId fieldId ) {
		super(
				baseDefinition,
				fieldId);
	}

	@Override
	public NumericData getNumericData(
			final GeometryWrapper geometry ) {
		return GeometryUtils.yRangeFromGeometry(geometry.getGeometry());
	}
}
