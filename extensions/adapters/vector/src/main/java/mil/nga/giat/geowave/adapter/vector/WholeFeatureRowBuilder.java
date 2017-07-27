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

import org.opengis.feature.simple.SimpleFeature;

public class WholeFeatureRowBuilder implements
		RowBuilder<SimpleFeature, Object>
{
	private SimpleFeature feature;

	@Override
	public void setField(
			PersistentValue<Object> fieldValue ) {
		feature = (SimpleFeature) fieldValue.getValue();
	}

	@Override
	public SimpleFeature buildRow(
			ByteArrayId dataId ) {
		return feature;
	}

}
