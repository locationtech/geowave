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
package mil.nga.giat.geowave.adapter.vector;

import java.util.Map;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;

/**
 * A GeoWave RowBuilder, used internally by AbstractDataAdapter to construct
 * rows from a set field values (in this case SimpleFeatures from a set of
 * attribute values). This implementation simply wraps a geotools
 * SimpleFeatureBuilder.
 * 
 */
public class AvroAttributeRowBuilder implements
		RowBuilder<SimpleFeature, Object>
{
	private Object object;

	public AvroAttributeRowBuilder() {}

	@Override
	public SimpleFeature buildRow(
			final ByteArrayId dataId ) {
		SimpleFeature sf = (SimpleFeature) object;
		return sf;
	}

	@Override
	public void setField(
			ByteArrayId id,
			Object fieldValue ) {
		// Only interested in the relevant fields
		if (id.equals(AvroFeatureAttributeHandler.FIELD_ID)) {
			object = fieldValue;
		}

	}

	@Override
	public void setFields(
			Map<ByteArrayId, Object> values ) {
		if (values.containsKey(AvroFeatureAttributeHandler.FIELD_ID)) {
			object = values.get(AvroFeatureAttributeHandler.FIELD_ID);
		}
	}
}
