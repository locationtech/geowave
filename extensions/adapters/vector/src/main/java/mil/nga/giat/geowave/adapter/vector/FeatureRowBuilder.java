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

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * A GeoWave RowBuilder, used internally by AbstractDataAdapter to construct
 * rows from a set field values (in this case SimpleFeatures from a set of
 * attribute values). This implementation simply wraps a geotools
 * SimpleFeatureBuilder.
 * 
 */
public class FeatureRowBuilder implements
		RowBuilder<SimpleFeature, Object>
{
	private final SimpleFeatureBuilder builder;

	public FeatureRowBuilder(
			final SimpleFeatureType type ) {
		builder = new SimpleFeatureBuilder(
				type);
	}

	@Override
	public SimpleFeature buildRow(
			final ByteArrayId dataId ) {
		return builder.buildFeature(dataId.getString());
	}

	@Override
	public void setField(
			final PersistentValue<Object> fieldValue ) {
		builder.set(
				fieldValue.getId().getString(),
				fieldValue.getValue());
	}

}
