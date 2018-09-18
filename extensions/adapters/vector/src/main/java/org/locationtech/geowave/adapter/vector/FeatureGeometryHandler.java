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
package org.locationtech.geowave.adapter.vector;

import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.store.adapter.IndexFieldHandler;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This class handles the internal responsibility of persisting JTS geometry to
 * and from a GeoWave common index field for SimpleFeature data.
 *
 */
public class FeatureGeometryHandler implements
		IndexFieldHandler<SimpleFeature, GeometryWrapper, Object>
{
	private final FeatureAttributeHandler nativeGeometryHandler;
	private final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler;
	private final String[] nativeFieldNames;

	public FeatureGeometryHandler(
			final AttributeDescriptor geometryAttrDesc ) {
		this(
				geometryAttrDesc,
				null);
	}

	public FeatureGeometryHandler(
			final AttributeDescriptor geometryAttrDesc,
			final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler ) {
		nativeGeometryHandler = new FeatureAttributeHandler(
				geometryAttrDesc);
		this.visibilityHandler = visibilityHandler;
		nativeFieldNames = new String[] {
			nativeGeometryHandler.getFieldName()
		};
	}

	@Override
	public String[] getNativeFieldNames() {
		return nativeFieldNames;
	}

	@Override
	public GeometryWrapper toIndexValue(
			final SimpleFeature row ) {
		final Geometry geometry = (Geometry) nativeGeometryHandler.getFieldValue(row);
		byte[] visibility;
		if (visibilityHandler != null) {
			visibility = visibilityHandler.getVisibility(
					row,
					nativeGeometryHandler.getFieldName(),
					geometry);
		}
		else {
			visibility = new byte[] {};
		}
		return new GeometryWrapper(
				geometry,
				visibility);
	}

	@SuppressWarnings("unchecked")
	@Override
	public PersistentValue<Object>[] toNativeValues(
			final GeometryWrapper indexValue ) {
		return new PersistentValue[] {
			new PersistentValue<>(
					nativeGeometryHandler.getFieldName(),
					indexValue.getGeometry())
		};
	}

}
