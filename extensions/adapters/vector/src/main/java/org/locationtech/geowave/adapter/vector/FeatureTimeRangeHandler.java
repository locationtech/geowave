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

import java.util.Arrays;

import org.locationtech.geowave.core.geotime.store.dimension.Time;
import org.locationtech.geowave.core.geotime.store.dimension.Time.TimeRange;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.store.adapter.IndexFieldHandler;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.opengis.feature.simple.SimpleFeature;

/**
 * This class handles the internal responsibility of persisting time ranges
 * based on a start time attribute and end time attribute to and from a GeoWave
 * common index field for SimpleFeature data.
 * 
 */
public class FeatureTimeRangeHandler implements
		IndexFieldHandler<SimpleFeature, Time, Object>
{
	private final FeatureAttributeHandler nativeStartTimeHandler;
	private final FeatureAttributeHandler nativeEndTimeHandler;
	private final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler;
	private final String[] nativeFieldNames;

	public FeatureTimeRangeHandler(
			final FeatureAttributeHandler nativeStartTimeHandler,
			final FeatureAttributeHandler nativeEndTimeHandler ) {
		this(
				nativeStartTimeHandler,
				nativeEndTimeHandler,
				null);
	}

	public FeatureTimeRangeHandler(
			final FeatureAttributeHandler nativeStartTimeHandler,
			final FeatureAttributeHandler nativeEndTimeHandler,
			final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler ) {
		this.nativeStartTimeHandler = nativeStartTimeHandler;
		this.nativeEndTimeHandler = nativeEndTimeHandler;
		this.visibilityHandler = visibilityHandler;
		nativeFieldNames = new String[] {
			nativeStartTimeHandler.getFieldName(),
			nativeEndTimeHandler.getFieldName()
		};
	}

	@Override
	public String[] getNativeFieldNames() {
		return nativeFieldNames;
	}

	@Override
	public Time toIndexValue(
			final SimpleFeature row ) {
		final Object startObj = nativeStartTimeHandler.getFieldValue(row);
		final Object endObj = nativeEndTimeHandler.getFieldValue(row);
		byte[] visibility;
		if (visibilityHandler != null) {
			final byte[] startVisibility = visibilityHandler.getVisibility(
					row,
					nativeStartTimeHandler.getFieldName(),
					startObj);
			final byte[] endVisibility = visibilityHandler.getVisibility(
					row,
					nativeEndTimeHandler.getFieldName(),
					endObj);
			if (Arrays.equals(
					startVisibility,
					endVisibility)) {
				// its easy if they both have the same visibility
				visibility = startVisibility;
			}
			else {
				// otherwise the assumption is that we combine the two
				// visibilities
				// TODO make sure this is how we should handle this case
				visibility = ByteArrayUtils.combineArrays(
						startVisibility,
						endVisibility);
			}
		}
		else {
			visibility = new byte[] {};
		}
		return new TimeRange(
				TimeUtils.getTimeMillis(startObj),
				TimeUtils.getTimeMillis(endObj),
				visibility);
	}

	@SuppressWarnings("unchecked")
	@Override
	public PersistentValue<Object>[] toNativeValues(
			final Time indexValue ) {
		final NumericData value = indexValue.toNumericData();
		final Class<?> startBindingClass = nativeStartTimeHandler.attrDesc.getType().getBinding();
		final Object startObj = TimeUtils.getTimeValue(
				startBindingClass,
				(long) value.getMin());
		final Class<?> endBindingClass = nativeEndTimeHandler.attrDesc.getType().getBinding();
		final Object endObj = TimeUtils.getTimeValue(
				endBindingClass,
				(long) value.getMax());
		return new PersistentValue[] {
			new PersistentValue<Object>(
					nativeStartTimeHandler.getFieldName(),
					startObj),
			new PersistentValue<Object>(
					nativeEndTimeHandler.getFieldName(),
					endObj),
		};
	}

}
