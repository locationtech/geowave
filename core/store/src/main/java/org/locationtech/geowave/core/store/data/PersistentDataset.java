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

import java.util.HashMap;
import java.util.Map;

/**
 * This is a basic mapping of field ID to native field type. "Native" in this
 * sense can be to either the data adapter or the common index, depending on
 * whether it is in the common index or is an extended field.
 *
 * @param <T>
 *            The most specific generalization for the type for all of the
 *            values in this dataset.
 */
public class PersistentDataset<T>
{
	private final Map<String, T> fieldNameToValueMap;

	public PersistentDataset() {
		fieldNameToValueMap = new HashMap<>();
	}

	public PersistentDataset(
			final String fieldName,
			final T value ) {
		this();
		addValue(
				fieldName,
				value);
	}

	public PersistentDataset(
			final Map<String, T> fieldIdToValueMap ) {
		this.fieldNameToValueMap = fieldIdToValueMap;
	}

	/**
	 * Add the field ID/value pair to this data set. Do not overwrite.
	 *
	 * @param value
	 *            the field ID/value pair to add
	 */
	public void addValue(
			final String fieldName,
			final T value ) {
		fieldNameToValueMap.put(
				fieldName,
				value);
	}

	/**
	 * Add several values to the data set.
	 */
	public void addValues(
			final Map<String, T> values ) {
		fieldNameToValueMap.putAll(values);
	}

	/**
	 * Given a field ID, get the associated value
	 *
	 * @param fieldName
	 *            the field ID
	 * @return the stored field value, null if this does not contain a value for
	 *         the ID
	 */
	public T getValue(
			final String fieldName ) {
		return fieldNameToValueMap.get(fieldName);
	}

	/**
	 * Get all of the values from this persistent data set
	 *
	 * @return all of the value
	 */
	public Map<String, T> getValues() {
		return fieldNameToValueMap;
	}
}
