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
package org.locationtech.geowave.core.geotime.store.query;

import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

import com.vividsolutions.jts.geom.Geometry;

public class IndexOnlySpatialQuery extends
		SpatialQuery
{
	public IndexOnlySpatialQuery() {
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

	public IndexOnlySpatialQuery(
			final Geometry queryGeometry,
			final String crsCode ) {
		super(
				queryGeometry,
				crsCode);
	}

	@Override
	protected QueryFilter createQueryFilter(
			final MultiDimensionalNumericData constraints,
			final NumericDimensionField<?>[] orderedConstrainedDimensionFields,
			final NumericDimensionField<?>[] unconstrainedDimensionDefinitions,
			final Index index ) {
		// this will ignore fine grained filters and just use the row ID in the
		// index
		return null;
	}

}
