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

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;

/**
 * This is responsible for persisting adapter/index mappings (either in memory
 * or to disk depending on the implementation).
 */
public interface AdapterIndexMappingStore
{
	public AdapterToIndexMapping getIndicesForAdapter(
			short internalAdapterId );

	/**
	 * If an adapter is already associated with indices and the provided indices
	 * do not match, returns false. Adapter can only be associated with a set of
	 * indices once!
	 * 
	 * @param adapter
	 * @param indices
	 * @throws MismatchedIndexToAdapterMapping
	 *             if the provided associations does match the stored
	 *             associations, if they exist
	 */
	public void addAdapterIndexMapping(
			AdapterToIndexMapping mapping )
			throws MismatchedIndexToAdapterMapping;

	/**
	 * Adapter to index mappings are maintain without regard to visibility
	 * constraints.
	 * 
	 * @param adapterId
	 */
	public void remove(
			short adapterId );

	public void removeAll();
}
