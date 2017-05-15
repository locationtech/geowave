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
package mil.nga.giat.geowave.analytic.extract;

import java.util.Iterator;

import mil.nga.giat.geowave.adapter.vector.FeatureGeometryHandler;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Geometry;

/**
 * 
 * Extract a Geometry from a Simple Feature.
 * 
 */
public class SimpleFeatureGeometryExtractor extends
		EmptyDimensionExtractor<SimpleFeature> implements
		DimensionExtractor<SimpleFeature>
{
	@Override
	public Geometry getGeometry(
			final SimpleFeature anObject ) {
		final FeatureGeometryHandler handler = new FeatureGeometryHandler(
				anObject.getDefaultGeometryProperty().getDescriptor());
		final Geometry geometry = handler.toIndexValue(
				anObject).getGeometry();
		final int srid = getSRID(anObject);
		geometry.setSRID(srid);
		return geometry;
	}

	protected static int getSRID(
			final SimpleFeature geometryFeature ) {
		final CoordinateReferenceSystem crs = geometryFeature
				.getDefaultGeometryProperty()
				.getDescriptor()
				.getCoordinateReferenceSystem();
		if (crs == null) {
			return 4326;
		}
		final ReferenceIdentifier id = getFirst(crs.getIdentifiers());
		if (id == null) {
			return 4326;
		}
		return Integer.parseInt(id.getCode());
	}

	protected static final <T> ReferenceIdentifier getFirst(
			final Iterable<ReferenceIdentifier> iterable ) {
		if (iterable == null) {
			return null;
		}
		final Iterator<ReferenceIdentifier> it = iterable.iterator();
		if (it.hasNext()) {
			final ReferenceIdentifier id = it.next();
			if ("EPSG".equals(id.getCodeSpace())) {
				return id;
			}
		}
		return null;
	}

	@Override
	public String getGroupID(
			SimpleFeature anObject ) {
		final Object v = anObject.getAttribute("GroupID");
		return v == null ? null : v.toString();
	}

}
