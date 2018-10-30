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
package org.locationtech.geowave.core.store.data;

/**
 * This represents a single value in the GeoWave data store as the value plus
 * the field ID pair
 *
 * @param <T>
 *            The binding class for this value
 */
public class PersistentValue<T>
{
	private final String fieldName;
	private final T value;

	public PersistentValue(
			final String fieldName,
			final T value ) {
		this.fieldName = fieldName;
		this.value = value;
	}

	/**
	 * Return the field name
	 *
	 * @return the field name
	 */
	public String getFieldName() {
		return fieldName;
	}

	/**
	 * Return the value
	 *
	 * @return the value
	 */
	public T getValue() {
		return value;
	}
}
