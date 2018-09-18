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

import org.locationtech.geowave.core.geotime.store.dimension.Time;
import org.locationtech.geowave.core.geotime.store.dimension.Time.Timestamp;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.adapter.IndexFieldHandler;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * This class handles the internal responsibility of persisting single timestamp
 * instants based on a temporal attribute (a Java binding class of Date or
 * Calendar for an attribute)to and from a GeoWave common index field for
 * SimpleFeature data.
 *
 */
public class FeatureTimestampHandler implements
		IndexFieldHandler<SimpleFeature, Time, Object>
{
	private final FeatureAttributeHandler nativeTimestampHandler;
	private final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler;
	private final String[] nativeFieldNames;

	public FeatureTimestampHandler(
			final AttributeDescriptor timestampAttrDesc ) {
		this(
				timestampAttrDesc,
				null);
	}

	public FeatureTimestampHandler(
			final AttributeDescriptor timestampAttrDesc,
			final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler ) {
		nativeTimestampHandler = new FeatureAttributeHandler(
				timestampAttrDesc);
		this.visibilityHandler = visibilityHandler;
		nativeFieldNames = new String[] {
			nativeTimestampHandler.getFieldName()
		};
	}

	@Override
	public String[] getNativeFieldNames() {
		return nativeFieldNames;
	}

	@Override
	public Time toIndexValue(
			final SimpleFeature row ) {
		final Object object = nativeTimestampHandler.getFieldValue(row);
		byte[] visibility;
		if (visibilityHandler != null) {
			visibility = visibilityHandler.getVisibility(
					row,
					nativeTimestampHandler.getFieldName(),
					object);
		}
		else {
			visibility = new byte[] {};
		}
		return new Timestamp(
				TimeUtils.getTimeMillis(object),
				visibility);
	}

	@SuppressWarnings("unchecked")
	@Override
	public PersistentValue<Object>[] toNativeValues(
			final Time indexValue ) {
		final Class<?> bindingClass = nativeTimestampHandler.attrDesc.getType().getBinding();
		final Object obj = TimeUtils.getTimeValue(
				bindingClass,
				(long) indexValue.toNumericData().getCentroid());
		return new PersistentValue[] {
			new PersistentValue<>(
					nativeTimestampHandler.getFieldName(),
					obj)
		};
	}
}
