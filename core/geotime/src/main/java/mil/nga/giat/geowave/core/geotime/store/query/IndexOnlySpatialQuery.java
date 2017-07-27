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
package mil.nga.giat.geowave.core.geotime.store.query;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

public class IndexOnlySpatialQuery extends
		SpatialQuery
{
	protected IndexOnlySpatialQuery() {
		super();
	}

	public IndexOnlySpatialQuery(
			final Constraints constraints,
			final Geometry queryGeometry ) {
		super(
				constraints,
				queryGeometry);
	}

	public IndexOnlySpatialQuery(
			final Geometry queryGeometry ) {
		super(
				queryGeometry);
	}

	@Override
	protected DistributableQueryFilter createQueryFilter(
			MultiDimensionalNumericData constraints,
			NumericDimensionField<?>[] orderedConstrainedDimensionFields,
			NumericDimensionField<?>[] unconstrainedDimensionDefinitions ) {
		// this will ignore fine grained filters and just use the row ID in the
		// index
		return null;
	}

}
