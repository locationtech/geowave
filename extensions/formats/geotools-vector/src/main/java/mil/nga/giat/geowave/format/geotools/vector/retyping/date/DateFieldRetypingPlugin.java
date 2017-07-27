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
package mil.nga.giat.geowave.format.geotools.vector.retyping.date;

import java.util.Map;

import mil.nga.giat.geowave.format.geotools.vector.RetypingVectorDataPlugin;

import org.opengis.feature.simple.SimpleFeatureType;

public class DateFieldRetypingPlugin implements
		RetypingVectorDataPlugin
{

	private final DateFieldOptionProvider dateFieldOptionProvider;

	public DateFieldRetypingPlugin(
			final DateFieldOptionProvider dateFieldOptionProvider ) {
		this.dateFieldOptionProvider = dateFieldOptionProvider;
	}

	@Override
	public RetypingVectorDataSource getRetypingSource(
			final SimpleFeatureType type ) {

		final Map<String, String> fieldNameToTimestampFormat = dateFieldOptionProvider.getFieldToFormatMap();

		RetypingVectorDataSource retypingSource = null;
		if (fieldNameToTimestampFormat != null && !fieldNameToTimestampFormat.isEmpty()) {
			retypingSource = new DateFieldRetypingSource(
					type,
					fieldNameToTimestampFormat);
		}
		return retypingSource;
	}
}
