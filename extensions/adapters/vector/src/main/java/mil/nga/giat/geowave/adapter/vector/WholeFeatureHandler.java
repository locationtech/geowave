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
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class WholeFeatureHandler implements
		NativeFieldHandler<SimpleFeature, Object>
{
	private final ByteArrayId fieldId;

	public WholeFeatureHandler(
			SimpleFeatureType type ) {
		super();
		fieldId = new ByteArrayId(
				StringUtils.stringToBinary(type.getTypeName()));
	}

	@Override
	public ByteArrayId getFieldId() {
		return fieldId;
	}

	@Override
	public Object getFieldValue(
			SimpleFeature row ) {
		return row;
	}

}
