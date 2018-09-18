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
import org.locationtech.geowave.core.store.CloseableIterator;

/**
 * This is responsible for persisting adapter/Internal Adapter mappings (either
 * in memory or to disk depending on the implementation).
 */
public interface InternalAdapterStore
{

	public CloseableIterator<ByteArrayId> getAdapterIds();

	public CloseableIterator<Short> getInternalAdapterIds();

	public ByteArrayId getAdapterId(
			short internalAdapterId );

	public Short getInternalAdapterId(
			ByteArrayId adapterId );

	/**
	 * If an adapter is already associated with an internal Adapter returns
	 * false. Adapter can only be associated with internal adapter once.
	 *
	 *
	 * @param adapterId
	 *            the adapter
	 * @return the internal ID
	 */
	public short addAdapterId(
			ByteArrayId adapterId );

	/**
	 * Adapter Id to Internal Adapter Id mappings are maintain without regard to
	 * visibility constraints.
	 *
	 * @param adapterId
	 */
	public boolean remove(
			ByteArrayId adapterId );

	public boolean remove(
			short internalAdapterId );

	public void removeAll();
}
