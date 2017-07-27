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
package mil.nga.giat.geowave.core.store.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;

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
	private final Map<ByteArrayId, T> fieldIdToValueMap;

	public PersistentDataset() {
		fieldIdToValueMap = new HashMap<ByteArrayId, T>();
	}

	public PersistentDataset(
			final PersistentValue<T> value ) {
		this();
		addValue(value);
	}

	public PersistentDataset(
			final Map<ByteArrayId, T> fieldIdToValueMap ) {
		this.fieldIdToValueMap = fieldIdToValueMap;
	}

	/**
	 * Add the field ID/value pair to this data set. Do not overwrite.
	 * 
	 * @param value
	 *            the field ID/value pair to add
	 */
	public void addValue(
			final PersistentValue<T> value ) {
		fieldIdToValueMap.put(
				value.getId(),
				value.getValue());
	}

	/**
	 * Given a field ID, get the associated value
	 * 
	 * @param fieldId
	 *            the field ID
	 * @return the stored field value, null if this does not contain a value for
	 *         the ID
	 */
	public T getValue(
			final ByteArrayId fieldId ) {
		return fieldIdToValueMap.get(fieldId);
	}

	/**
	 * Get all of the values from this persistent data set
	 * 
	 * @return all of the value
	 */
	public List<PersistentValue<T>> getValues() {
		final List<PersistentValue<T>> values = new ArrayList<PersistentValue<T>>(
				fieldIdToValueMap.size());
		for (final Entry<ByteArrayId, T> entry : fieldIdToValueMap.entrySet()) {
			values.add(new PersistentValue<T>(
					entry.getKey(),
					entry.getValue()));
		}
		return values;
	}
}
