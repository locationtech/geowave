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
package org.locationtech.geowave.core.store.adapter;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * This is responsible for persisting data adapters (either in memory or to disk
 * depending on the implementation).
 */
public interface AdapterStore<K, V extends DataTypeAdapter<?>>
{
	/**
	 * Add the adapter to the store
	 *
	 * @param adapter
	 *            the adapter
	 */
	public void addAdapter(
			V adapter );

	/**
	 * Get an adapter from the store by its unique ID
	 *
	 * @param adapterId
	 *            the unique adapter ID
	 * @return the adapter, null if it doesn't exist
	 */
	public V getAdapter(
			K adapterId );

	/**
	 * Check for the existence of the adapter with the given unique ID
	 *
	 * @param adapterId
	 *            the unique ID to look up
	 * @return a boolean flag indicating whether the adapter exists
	 */
	public boolean adapterExists(
			K adapterId );

	/**
	 * Get the full set of adapters within this store
	 *
	 * @return an iterator over all of the adapters in this store
	 */
	public CloseableIterator<V> getAdapters();

	public void removeAll();

	/**
	 *
	 * @param adapterId
	 *            the adapter ID to remove
	 */
	public void removeAdapter(
			K adapterId );
}
