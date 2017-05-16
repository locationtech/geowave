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

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * This represents a single value in the GeoWave data store as the value plus
 * the field ID pair
 * 
 * @param <T>
 *            The binding class for this value
 */
public class PersistentValue<T>
{
	private final ByteArrayId id;
	private final T value;

	public PersistentValue(
			final ByteArrayId id,
			final T value ) {
		this.id = id;
		this.value = value;
	}

	/**
	 * Return the field ID
	 * 
	 * @return the field ID
	 */
	public ByteArrayId getId() {
		return id;
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
