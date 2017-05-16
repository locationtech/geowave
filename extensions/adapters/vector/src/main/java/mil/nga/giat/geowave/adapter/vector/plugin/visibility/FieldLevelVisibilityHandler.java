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
package mil.nga.giat.geowave.adapter.vector.plugin.visibility;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.VisibilityManagement;

import org.opengis.feature.simple.SimpleFeature;

/**
 * Define visibility for a specific attribute using the
 * {@link VisibilityManagement}. The visibility is determined by meta-data in a
 * separate feature attribute.
 * 
 * @see JsonDefinitionColumnVisibilityManagement
 * 
 * 
 * 
 * @param <T>
 * @param <CommonIndexValue>
 */
public abstract class FieldLevelVisibilityHandler<T, CommonIndexValue> implements
		FieldVisibilityHandler<T, CommonIndexValue>
{

	private final String visibilityAttribute;
	private final String fieldName;
	private FieldVisibilityHandler<T, Object> defaultFieldVisiblityHandler;

	/**
	 * Used when acting with an Index adaptor as a visibility handler. This
	 * 
	 * @param fieldName
	 *            - the name of the field for which to set determine the
	 *            visibility.
	 * @param fieldVisiblityHandler
	 *            default visibility handler if a specific visibility cannot be
	 *            determined from the contents of the attribute used to
	 *            determine visibility (name providied by parameter
	 *            'visibilityAttribute')
	 * @param visibilityAttribute
	 *            the attribute name that contains data to discern visibility
	 *            for other field/attributes.
	 * @param visibilityManagement
	 */
	public FieldLevelVisibilityHandler(
			final String fieldName,
			final FieldVisibilityHandler<T, Object> fieldVisiblityHandler,
			final String visibilityAttribute ) {
		super();
		this.fieldName = fieldName;
		this.visibilityAttribute = visibilityAttribute;
		this.defaultFieldVisiblityHandler = fieldVisiblityHandler;
	}

	/**
	 * 
	 * @param visibilityObject
	 *            an object that defines visibility for each field
	 * @param fieldName
	 *            the field to which visibility is being requested
	 * @return null if the default should be used, otherwise return the
	 *         visibility for the provide field given the instructions found in
	 *         the visibilityObject
	 */
	public abstract byte[] translateVisibility(
			final Object visibilityObject,
			final String fieldName );

	@Override
	public byte[] getVisibility(
			T rowValue,
			ByteArrayId fieldId,
			CommonIndexValue fieldValue ) {

		SimpleFeature feature = (SimpleFeature) rowValue;
		Object visibilityAttributeValue = feature.getAttribute(this.visibilityAttribute);
		final byte[] result = visibilityAttributeValue != null ? translateVisibility(
				visibilityAttributeValue,
				fieldName) : null;
		return result != null ? result : (defaultFieldVisiblityHandler == null ? new byte[0]
				: defaultFieldVisiblityHandler.getVisibility(
						rowValue,
						fieldId,
						fieldValue));
	}

}
