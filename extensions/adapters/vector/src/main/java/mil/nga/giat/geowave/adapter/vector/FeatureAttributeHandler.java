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
import org.opengis.feature.type.AttributeDescriptor;

/**
 * This is used by the FeatureDataAdapter to handle GeoWave 'fields' using
 * SimpleFeature 'attributes.'
 * 
 */
public class FeatureAttributeHandler implements
		NativeFieldHandler<SimpleFeature, Object>
{
	protected final AttributeDescriptor attrDesc;
	private final ByteArrayId fieldId;

	public FeatureAttributeHandler(
			final AttributeDescriptor attrDesc ) {
		this.attrDesc = attrDesc;
		fieldId = new ByteArrayId(
				StringUtils.stringToBinary(attrDesc.getLocalName()));
	}

	@Override
	public ByteArrayId getFieldId() {
		return fieldId;
	}

	@Override
	public Object getFieldValue(
			final SimpleFeature row ) {
		return row.getAttribute(attrDesc.getName());
	}
}
