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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.opengis.feature.simple.SimpleFeature;

public class AvroFeatureReader implements
		FieldReader<Object>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AvroFeatureReader.class);

	@Override
	public Object readField(
			final byte[] fieldData ) {
		SimpleFeature deserializedSimpleFeature = null;
		try {
			deserializedSimpleFeature = AvroFeatureUtils.deserializeAvroSimpleFeature(fieldData);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to deserialize SimpleFeature",
					e);
		}

		return deserializedSimpleFeature;
	}

}
